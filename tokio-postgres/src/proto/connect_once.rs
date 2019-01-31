#![allow(clippy::large_enum_variant)]

use fallible_iterator::FallibleIterator;
use futures::{try_ready, Async, Future, Poll, Stream};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::io;

use crate::proto::{
    Client, ConnectRawFuture, ConnectSocketFuture, Connection, MaybeTlsStream, SimpleQueryStream, Request
};
use crate::{Channel, Config, Error, Socket, TargetSessionAttrs, TlsConnect};

#[derive(StateMachineFuture)]
pub enum ConnectOnce<T, C, MC>
where
    T: TlsConnect<Socket>,
    C: Channel<Request>,
    MC: Fn() -> C + Clone,
{
    #[state_machine_future(start, transitions(ConnectingSocket))]
    Start { idx: usize, tls: T, config: Config, make_ch: MC },
    #[state_machine_future(transitions(ConnectingRaw))]
    ConnectingSocket {
        future: ConnectSocketFuture,
        idx: usize,
        tls: T,
        config: Config,
        make_ch: MC,
    },
    #[state_machine_future(transitions(CheckingSessionAttrs, Finished))]
    ConnectingRaw {
        future: ConnectRawFuture<Socket, T, C, MC>,
        target_session_attrs: TargetSessionAttrs,
    },
    #[state_machine_future(transitions(Finished))]
    CheckingSessionAttrs {
        stream: SimpleQueryStream,
        client: Client<C>,
        connection: Connection<MaybeTlsStream<Socket, T::Stream>, C>,
    },
    #[state_machine_future(ready)]
    Finished((Client<C>, Connection<MaybeTlsStream<Socket, T::Stream>, C>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T, C, MC> PollConnectOnce<T, C, MC> for ConnectOnce<T, C, MC>
where
    T: TlsConnect<Socket>,
    C: Channel<Request>,
    MC: Fn() -> C + Clone,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T, C, MC>>) -> Poll<AfterStart<T, C, MC>, Error> {
        let state = state.take();

        transition!(ConnectingSocket {
            future: ConnectSocketFuture::new(state.config.clone(), state.idx),
            idx: state.idx,
            tls: state.tls,
            config: state.config,
            make_ch: state.make_ch,
        })
    }

    fn poll_connecting_socket<'a>(
        state: &'a mut RentToOwn<'a, ConnectingSocket<T, C, MC>>,
    ) -> Poll<AfterConnectingSocket<T, C, MC>, Error> {
        let socket = try_ready!(state.future.poll());
        let state = state.take();

        transition!(ConnectingRaw {
            target_session_attrs: state.config.0.target_session_attrs,
            future: ConnectRawFuture::new(socket, state.tls, state.config, Some(state.idx), state.make_ch),
        })
    }

    fn poll_connecting_raw<'a>(
        state: &'a mut RentToOwn<'a, ConnectingRaw<T, C, MC>>,
    ) -> Poll<AfterConnectingRaw<T, C>, Error> {
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
        state: &'a mut RentToOwn<'a, CheckingSessionAttrs<T, C>>,
    ) -> Poll<AfterCheckingSessionAttrs<T, C>, Error> {
        if let Async::Ready(()) = state.connection.poll()? {
            return Err(Error::closed());
        }

        match try_ready!(state.stream.poll()) {
            Some(row) => {
                let range = row.ranges().next().map_err(Error::parse)?.and_then(|r| r);
                if range.map(|r| &row.buffer()[r]) == Some(b"on") {
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

impl<T, C, MC> ConnectOnceFuture<T, C, MC>
where
    T: TlsConnect<Socket>,
    C: Channel<Request>,
    MC: Fn() -> C + Clone,
{
    pub fn new(idx: usize, tls: T, config: Config, make_ch: MC) -> ConnectOnceFuture<T, C, MC> {
        ConnectOnce::start(idx, tls, config, make_ch)
    }
}
