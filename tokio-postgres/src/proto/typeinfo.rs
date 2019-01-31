use futures::stream::{self, Stream};
use futures::{try_ready, Async, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::Channel;
use crate::proto::Request;
use crate::error::{Error, SqlState};
use crate::next_statement;
use crate::proto::client::Client;
use crate::proto::prepare::PrepareFuture;
use crate::proto::query::QueryStream;
use crate::proto::statement::Statement;
use crate::proto::typeinfo_composite::TypeinfoCompositeFuture;
use crate::proto::typeinfo_enum::TypeinfoEnumFuture;
use crate::types::{Kind, Oid, Type};

const TYPEINFO_QUERY: &str = "
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
";

// Range types weren't added until Postgres 9.2, so pg_range may not exist
const TYPEINFO_FALLBACK_QUERY: &str = "
SELECT t.typname, t.typtype, t.typelem, NULL::OID, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
";

#[derive(StateMachineFuture)]
pub enum Typeinfo<C>
where
    C: Channel<Request>,
{
    #[state_machine_future(start, transitions(PreparingTypeinfo, QueryingTypeinfo, Finished))]
    Start { oid: Oid, client: Client<C> },
    #[state_machine_future(transitions(PreparingTypeinfoFallback, QueryingTypeinfo))]
    PreparingTypeinfo {
        future: Box<PrepareFuture<C>>,
        oid: Oid,
        client: Client<C>,
    },
    #[state_machine_future(transitions(QueryingTypeinfo))]
    PreparingTypeinfoFallback {
        future: Box<PrepareFuture<C>>,
        oid: Oid,
        client: Client<C>,
    },
    #[state_machine_future(transitions(
        CachingType,
        QueryingEnumVariants,
        QueryingDomainBasetype,
        QueryingArrayElem,
        QueryingCompositeFields,
        QueryingRangeSubtype
    ))]
    QueryingTypeinfo {
        future: stream::Collect<QueryStream<Statement<C>, C>>,
        oid: Oid,
        client: Client<C>,
    },
    #[state_machine_future(transitions(CachingType))]
    QueryingEnumVariants {
        future: TypeinfoEnumFuture<C>,
        name: String,
        oid: Oid,
        schema: String,
    },
    #[state_machine_future(transitions(CachingType))]
    QueryingDomainBasetype {
        future: Box<TypeinfoFuture<C>>,
        name: String,
        oid: Oid,
        schema: String,
    },
    #[state_machine_future(transitions(CachingType))]
    QueryingArrayElem {
        future: Box<TypeinfoFuture<C>>,
        name: String,
        oid: Oid,
        schema: String,
    },
    #[state_machine_future(transitions(CachingType))]
    QueryingCompositeFields {
        future: TypeinfoCompositeFuture<C>,
        name: String,
        oid: Oid,
        schema: String,
    },
    #[state_machine_future(transitions(CachingType))]
    QueryingRangeSubtype {
        future: Box<TypeinfoFuture<C>>,
        name: String,
        oid: Oid,
        schema: String,
    },
    #[state_machine_future(transitions(Finished))]
    CachingType { ty: Type, oid: Oid, client: Client<C> },
    #[state_machine_future(ready)]
    Finished((Type, Client<C>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<C> PollTypeinfo<C> for Typeinfo<C>
where
    C: Channel<Request>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<C>>) -> Poll<AfterStart<C>, Error> {
        let state = state.take();

        if let Some(ty) = Type::from_oid(state.oid) {
            transition!(Finished((ty, state.client)));
        }

        if let Some(ty) = state.client.cached_type(state.oid) {
            transition!(Finished((ty, state.client)));
        }

        match state.client.typeinfo_query() {
            Some(statement) => transition!(QueryingTypeinfo {
                future: state.client.query(&statement, &[&state.oid]).collect(),
                oid: state.oid,
                client: state.client,
            }),
            None => transition!(PreparingTypeinfo {
                future: Box::new(state.client.prepare(next_statement(), TYPEINFO_QUERY, &[])),
                oid: state.oid,
                client: state.client,
            }),
        }
    }

    fn poll_preparing_typeinfo<'a>(
        state: &'a mut RentToOwn<'a, PreparingTypeinfo<C>>,
    ) -> Poll<AfterPreparingTypeinfo<C>, Error> {
        let statement = match state.future.poll() {
            Ok(Async::Ready(statement)) => statement,
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(ref e) if e.code() == Some(&SqlState::UNDEFINED_TABLE) => {
                let state = state.take();

                transition!(PreparingTypeinfoFallback {
                    future: Box::new(state.client.prepare(
                        next_statement(),
                        TYPEINFO_FALLBACK_QUERY,
                        &[]
                    )),
                    oid: state.oid,
                    client: state.client,
                })
            }
            Err(e) => return Err(e),
        };
        let state = state.take();

        let future = state.client.query(&statement, &[&state.oid]).collect();
        state.client.set_typeinfo_query(&statement);
        transition!(QueryingTypeinfo {
            future,
            oid: state.oid,
            client: state.client
        })
    }

    fn poll_preparing_typeinfo_fallback<'a>(
        state: &'a mut RentToOwn<'a, PreparingTypeinfoFallback<C>>,
    ) -> Poll<AfterPreparingTypeinfoFallback<C>, Error> {
        let statement = try_ready!(state.future.poll());
        let state = state.take();

        let future = state.client.query(&statement, &[&state.oid]).collect();
        state.client.set_typeinfo_query(&statement);
        transition!(QueryingTypeinfo {
            future,
            oid: state.oid,
            client: state.client
        })
    }

    fn poll_querying_typeinfo<'a>(
        state: &'a mut RentToOwn<'a, QueryingTypeinfo<C>>,
    ) -> Poll<AfterQueryingTypeinfo<C>, Error> {
        let rows = try_ready!(state.future.poll());
        let state = state.take();

        let row = match rows.get(0) {
            Some(row) => row,
            None => return Err(Error::unexpected_message()),
        };

        let name = row
            .try_get::<_, String>(0)?
            .ok_or_else(Error::unexpected_message)?;
        let type_ = row
            .try_get::<_, i8>(1)?
            .ok_or_else(Error::unexpected_message)?;
        let elem_oid = row
            .try_get::<_, Oid>(2)?
            .ok_or_else(Error::unexpected_message)?;
        let rngsubtype = row
            .try_get::<_, Option<Oid>>(3)?
            .ok_or_else(Error::unexpected_message)?;
        let basetype = row
            .try_get::<_, Oid>(4)?
            .ok_or_else(Error::unexpected_message)?;
        let schema = row
            .try_get::<_, String>(5)?
            .ok_or_else(Error::unexpected_message)?;
        let relid = row
            .try_get::<_, Oid>(6)?
            .ok_or_else(Error::unexpected_message)?;

        let kind = if type_ == b'e' as i8 {
            transition!(QueryingEnumVariants {
                future: TypeinfoEnumFuture::new(state.oid, state.client),
                name,
                oid: state.oid,
                schema,
            })
        } else if type_ == b'p' as i8 {
            Kind::Pseudo
        } else if basetype != 0 {
            transition!(QueryingDomainBasetype {
                future: Box::new(TypeinfoFuture::new(basetype, state.client)),
                name,
                oid: state.oid,
                schema,
            })
        } else if elem_oid != 0 {
            transition!(QueryingArrayElem {
                future: Box::new(TypeinfoFuture::new(elem_oid, state.client)),
                name,
                oid: state.oid,
                schema,
            })
        } else if relid != 0 {
            transition!(QueryingCompositeFields {
                future: TypeinfoCompositeFuture::new(relid, state.client),
                name,
                oid: state.oid,
                schema,
            })
        } else if let Some(rngsubtype) = rngsubtype {
            transition!(QueryingRangeSubtype {
                future: Box::new(TypeinfoFuture::new(rngsubtype, state.client)),
                name,
                oid: state.oid,
                schema,
            })
        } else {
            Kind::Simple
        };

        let ty = Type::_new(name.to_string(), state.oid, kind, schema.to_string());
        transition!(CachingType {
            ty,
            oid: state.oid,
            client: state.client,
        })
    }

    fn poll_querying_enum_variants<'a>(
        state: &'a mut RentToOwn<'a, QueryingEnumVariants<C>>,
    ) -> Poll<AfterQueryingEnumVariants<C>, Error> {
        let (variants, client) = try_ready!(state.future.poll());
        let state = state.take();

        let ty = Type::_new(state.name, state.oid, Kind::Enum(variants), state.schema);
        transition!(CachingType {
            ty,
            oid: state.oid,
            client,
        })
    }

    fn poll_querying_domain_basetype<'a>(
        state: &'a mut RentToOwn<'a, QueryingDomainBasetype<C>>,
    ) -> Poll<AfterQueryingDomainBasetype<C>, Error> {
        let (basetype, client) = try_ready!(state.future.poll());
        let state = state.take();

        let ty = Type::_new(state.name, state.oid, Kind::Domain(basetype), state.schema);
        transition!(CachingType {
            ty,
            oid: state.oid,
            client,
        })
    }

    fn poll_querying_array_elem<'a>(
        state: &'a mut RentToOwn<'a, QueryingArrayElem<C>>,
    ) -> Poll<AfterQueryingArrayElem<C>, Error> {
        let (elem, client) = try_ready!(state.future.poll());
        let state = state.take();

        let ty = Type::_new(state.name, state.oid, Kind::Array(elem), state.schema);
        transition!(CachingType {
            ty,
            oid: state.oid,
            client,
        })
    }

    fn poll_querying_composite_fields<'a>(
        state: &'a mut RentToOwn<'a, QueryingCompositeFields<C>>,
    ) -> Poll<AfterQueryingCompositeFields<C>, Error> {
        let (fields, client) = try_ready!(state.future.poll());
        let state = state.take();

        let ty = Type::_new(state.name, state.oid, Kind::Composite(fields), state.schema);
        transition!(CachingType {
            ty,
            oid: state.oid,
            client,
        })
    }

    fn poll_querying_range_subtype<'a>(
        state: &'a mut RentToOwn<'a, QueryingRangeSubtype<C>>,
    ) -> Poll<AfterQueryingRangeSubtype<C>, Error> {
        let (subtype, client) = try_ready!(state.future.poll());
        let state = state.take();

        let ty = Type::_new(state.name, state.oid, Kind::Range(subtype), state.schema);
        transition!(CachingType {
            ty,
            oid: state.oid,
            client,
        })
    }

    fn poll_caching_type<'a>(
        state: &'a mut RentToOwn<'a, CachingType<C>>,
    ) -> Poll<AfterCachingType<C>, Error> {
        let state = state.take();
        state.client.cache_type(&state.ty);
        transition!(Finished((state.ty, state.client)))
    }
}

impl<C> TypeinfoFuture<C>
where
    C: Channel<Request>,
{
    pub fn new(oid: Oid, client: Client<C>) -> TypeinfoFuture<C> {
        Typeinfo::start(oid, client)
    }
}
