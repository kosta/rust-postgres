#![allow(clippy::large_enum_variant)]

use futures::{try_ready, Async, Future, Poll, Stream};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::io;

use crate::proto::{Client, ConnectRawFuture, ConnectSocketFuture, Connection, SimpleQueryStream};
use crate::{Config, Error, Socket, TargetSessionAttrs, TlsMode};

#[derive(StateMachineFuture)]
pub enum ConnectOnce<T>
where
    T: TlsMode<Socket>,
{
    #[state_machine_future(start, transitions(ConnectingSocket))]
    Start {
        idx: usize,
        tls_mode: T,
        config: Config,
    },
    #[state_machine_future(transitions(ConnectingRaw))]
    ConnectingSocket {
        future: ConnectSocketFuture,
        idx: usize,
        tls_mode: T,
        config: Config,
    },
    #[state_machine_future(transitions(CheckingSessionAttrs, Finished))]
    ConnectingRaw {
        future: ConnectRawFuture<Socket, T>,
        target_session_attrs: TargetSessionAttrs,
    },
    #[state_machine_future(transitions(Finished))]
    CheckingSessionAttrs {
        stream: SimpleQueryStream,
        client: Client,
        connection: Connection<T::Stream>,
    },
    #[state_machine_future(ready)]
    Finished((Client, Connection<T::Stream>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T> PollConnectOnce<T> for ConnectOnce<T>
where
    T: TlsMode<Socket>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T>>) -> Poll<AfterStart<T>, Error> {
        let state = state.take();

        transition!(ConnectingSocket {
            future: ConnectSocketFuture::new(state.config.clone(), state.idx),
            idx: state.idx,
            tls_mode: state.tls_mode,
            config: state.config,
        })
    }

    fn poll_connecting_socket<'a>(
        state: &'a mut RentToOwn<'a, ConnectingSocket<T>>,
    ) -> Poll<AfterConnectingSocket<T>, Error> {
        let socket = try_ready!(state.future.poll());
        let state = state.take();

        transition!(ConnectingRaw {
            target_session_attrs: state.config.0.target_session_attrs,
            future: ConnectRawFuture::new(socket, state.tls_mode, state.config, Some(state.idx)),
        })
    }

    fn poll_connecting_raw<'a>(
        state: &'a mut RentToOwn<'a, ConnectingRaw<T>>,
    ) -> Poll<AfterConnectingRaw<T>, Error> {
        let (client, connection) = try_ready!(state.future.poll());

        if let TargetSessionAttrs::ReadWrite = state.target_session_attrs {
            transition!(CheckingSessionAttrs {
                stream: client.batch_execute("SHOW transaction_read_only"),
                client,
                connection,
            })
        } else {
            transition!(Finished((client, connection)))
        }
    }

    fn poll_checking_session_attrs<'a>(
        state: &'a mut RentToOwn<'a, CheckingSessionAttrs<T>>,
    ) -> Poll<AfterCheckingSessionAttrs<T>, Error> {
        if let Async::Ready(()) = state.connection.poll()? {
            return Err(Error::closed());
        }

        match try_ready!(state.stream.poll()) {
            Some(row) => {
                if row.get(0) == Some("on") {
                    Err(Error::connect(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "database does not allow writes",
                    )))
                } else {
                    let state = state.take();
                    transition!(Finished((state.client, state.connection)))
                }
            }
            None => Err(Error::closed()),
        }
    }
}

impl<T> ConnectOnceFuture<T>
where
    T: TlsMode<Socket>,
{
    pub fn new(idx: usize, tls_mode: T, config: Config) -> ConnectOnceFuture<T> {
        ConnectOnce::start(idx, tls_mode, config)
    }
}
