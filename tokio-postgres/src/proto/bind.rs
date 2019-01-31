use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::client::{Client, PendingRequest};
use crate::proto::portal::Portal;
use crate::proto::statement::Statement;
use crate::{Error, Channel};
use crate::proto::Request;

#[derive(StateMachineFuture)]
pub enum Bind<C: Channel<Request>> {
    #[state_machine_future(start, transitions(ReadBindComplete))]
    Start {
        client: Client<C>,
        request: PendingRequest,
        name: String,
        statement: Statement<C>,
    },
    #[state_machine_future(transitions(Finished))]
    ReadBindComplete {
        receiver: mpsc::Receiver<Message>,
        client: Client<C>,
        name: String,
        statement: Statement<C>,
    },
    #[state_machine_future(ready)]
    Finished(Portal<C>),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<C: Channel<Request>> PollBind<C> for Bind<C>
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<C>>) -> Poll<AfterStart<C>, Error> {
        let state = state.take();
        let receiver = state.client.send(state.request)?;

        transition!(ReadBindComplete {
            receiver,
            client: state.client,
            name: state.name,
            statement: state.statement,
        })
    }

    fn poll_read_bind_complete<'a>(
        state: &'a mut RentToOwn<'a, ReadBindComplete<C>>,
    ) -> Poll<AfterReadBindComplete<C>, Error> {
        let message = try_ready_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::BindComplete) => transition!(Finished(Portal::new(
                state.client.downgrade(),
                state.name,
                state.statement,
            ))),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }
}

impl<C: Channel<Request>> BindFuture<C> {
    pub fn new(
        client: Client<C>,
        request: PendingRequest,
        name: String,
        statement: Statement<C>,
    ) -> BindFuture<C> {
        Bind::start(client, request, name, statement)
    }
}
