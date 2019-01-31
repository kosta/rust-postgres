use futures::{Async, Future, Poll, Stream};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::{Client, ConnectOnceFuture, Connection, MaybeTlsStream, Request};
use crate::{Channel, Config, Error, Host, MakeTlsConnect, Socket};

#[derive(StateMachineFuture)]
pub enum Connect<T, C, MC>
where
    T: MakeTlsConnect<Socket>,
    C: Channel<Request>,
    MC: Fn() -> C + Clone,
{
    #[state_machine_future(start, transitions(Connecting))]
    Start {
        tls: T,
        config: Result<Config, Error>,
        make_ch: MC,
    },
    #[state_machine_future(transitions(Finished))]
    Connecting {
        future: ConnectOnceFuture<T::TlsConnect, C, MC>,
        idx: usize,
        tls: T,
        config: Config,
        make_ch: MC,
    },
    #[state_machine_future(ready)]
    Finished((Client<C>, Connection<MaybeTlsStream<Socket, T::Stream>, C>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T, C, MC> PollConnect<T, C, MC> for Connect<T, C, MC>
where
    T: MakeTlsConnect<Socket>,
    C: Channel<Request>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T, C, MC>>) -> Poll<AfterStart<T, C, MC>, Error> {
        let mut state = state.take();

        let config = state.config?;

        if config.0.host.is_empty() {
            return Err(Error::config("host missing".into()));
        }

        if config.0.port.len() > 1 && config.0.port.len() != config.0.host.len() {
            return Err(Error::config("invalid number of ports".into()));
        }

        let hostname = match &config.0.host[0] {
            Host::Tcp(host) => &**host,
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };
        let tls = state
            .tls
            .make_tls_connect(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        transition!(Connecting {
            future: ConnectOnceFuture::new(0, tls, config.clone(), state.make_ch.clone()),
            idx: 0,
            tls: state.tls,
            config,
            make_ch: state.make_ch,
        })
    }

    fn poll_connecting<'a>(
        state: &'a mut RentToOwn<'a, Connecting<T, C, MC>>,
    ) -> Poll<AfterConnecting<T, C, MC>, Error> {
        loop {
            match state.future.poll() {
                Ok(Async::Ready(r)) => transition!(Finished(r)),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    let state = &mut **state;
                    state.idx += 1;

                    let host = match state.config.0.host.get(state.idx) {
                        Some(host) => host,
                        None => return Err(e),
                    };

                    let hostname = match host {
                        Host::Tcp(host) => &**host,
                        #[cfg(unix)]
                        Host::Unix(_) => "",
                    };
                    let tls = state
                        .tls
                        .make_tls_connect(hostname)
                        .map_err(|e| Error::tls(e.into()))?;

                    state.future = ConnectOnceFuture::new(state.idx, tls, state.config.clone(), state.make_ch.clone());
                }
            }
        }
    }
}

impl<T, C, MC> ConnectFuture<T, C, MC>
where
    T: MakeTlsConnect<Socket>,
    C: Channel<Request>,
    MC: Fn() -> C + Clone,
{
    pub fn new(tls: T, config: Result<Config, Error>, make_ch: MC) -> ConnectFuture<T, C, MC> {
        Connect::start(tls, config, make_ch)
    }
}
