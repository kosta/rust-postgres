use crate::proto::client::Client;
use crate::proto::simple_query::SimpleQueryStream;
use futures::{try_ready, Async, Future, Poll, Stream};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::Request;
use crate::{Channel, Error};

#[derive(StateMachineFuture)]
pub enum Transaction<F, T, E, C>
where
    F: Future<Item = T, Error = E>,
    E: From<Error>,
    C: Channel<Request>,
{
    #[state_machine_future(start, transitions(Beginning))]
    Start { client: Client<C>, future: F },
    #[state_machine_future(transitions(Running))]
    Beginning {
        client: Client<C>,
        begin: SimpleQueryStream<C>,
        future: F,
    },
    #[state_machine_future(transitions(Finishing))]
    Running { client: Client<C>, future: F },
    #[state_machine_future(transitions(Finished))]
    Finishing {
        future: SimpleQueryStream<C>,
        result: Result<T, E>,
    },
    #[state_machine_future(ready)]
    Finished(T),
    #[state_machine_future(error)]
    Failed(E),
}

impl<F, T, E, C> PollTransaction<F, T, E, C> for Transaction<F, T, E, C>
where
    F: Future<Item = T, Error = E>,
    E: From<Error>,
    C: Channel<Request>,
{
    fn poll_start<'a>(
        state: &'a mut RentToOwn<'a, Start<F, T, E, C>>,
    ) -> Poll<AfterStart<F, T, E, C>, E> {
        let state = state.take();
        transition!(Beginning {
            begin: state.client.batch_execute("BEGIN"),
            client: state.client,
            future: state.future,
        })
    }

    fn poll_beginning<'a>(
        state: &'a mut RentToOwn<'a, Beginning<F, T, E, C>>,
    ) -> Poll<AfterBeginning<F, T, E, C>, E> {
        while let Some(_) = try_ready!(state.begin.poll()) {}

        let state = state.take();
        transition!(Running {
            client: state.client,
            future: state.future,
        })
    }

    fn poll_running<'a>(
        state: &'a mut RentToOwn<'a, Running<F, T, E, C>>,
    ) -> Poll<AfterRunning<T, E, C>, E> {
        match state.future.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(t)) => transition!(Finishing {
                future: state.client.batch_execute("COMMIT"),
                result: Ok(t),
            }),
            Err(e) => transition!(Finishing {
                future: state.client.batch_execute("ROLLBACK"),
                result: Err(e),
            }),
        }
    }

    fn poll_finishing<'a>(
        state: &'a mut RentToOwn<'a, Finishing<T, E, C>>,
    ) -> Poll<AfterFinishing<T>, E> {
        loop {
            match state.future.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(Some(_))) => {}
                Ok(Async::Ready(None)) => {
                    let t = state.take().result?;
                    transition!(Finished(t))
                }
                Err(e) => match state.take().result {
                    Ok(_) => return Err(e.into()),
                    // prioritize the future's error over the rollback error
                    Err(e) => return Err(e),
                },
            }
        }
    }
}

impl<F, T, E, C> TransactionFuture<F, T, E, C>
where
    F: Future<Item = T, Error = E>,
    E: From<Error>,
    C: Channel<Request>,
{
    pub fn new(client: Client<C>, future: F) -> TransactionFuture<F, T, E, C> {
        Transaction::start(client, future)
    }
}
