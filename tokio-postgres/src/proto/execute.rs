use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::client::{Client, PendingRequest};
use crate::proto::statement::Statement;
use crate::proto::Request;
use crate::{Channel, Error};

#[derive(StateMachineFuture)]
pub enum Execute<C>
where C: Channel<Request>,
{
    #[state_machine_future(start, transitions(ReadResponse))]
    Start {
        client: Client<C>,
        request: PendingRequest,
        statement: Statement,
    },
    #[state_machine_future(transitions(Finished))]
    ReadResponse { receiver: mpsc::Receiver<Message> },
    #[state_machine_future(ready)]
    Finished(u64),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<C> PollExecute<C> for Execute<C>
where
    C: Channel<Request>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<C>>) -> Poll<AfterStart, Error> {
        let state = state.take();
        let receiver = state.client.send(state.request)?;

        // the statement can drop after this point, since its close will queue up after the execution
        transition!(ReadResponse { receiver })
    }

    fn poll_read_response<'a>(
        state: &'a mut RentToOwn<'a, ReadResponse>,
    ) -> Poll<AfterReadResponse, Error> {
        loop {
            let message = try_ready_receive!(state.receiver.poll());

            match message {
                Some(Message::BindComplete) => {}
                Some(Message::DataRow(_)) => {}
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(Message::CommandComplete(body)) => {
                    let rows = body
                        .tag()
                        .map_err(Error::parse)?
                        .rsplit(' ')
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap_or(0);
                    transition!(Finished(rows))
                }
                Some(Message::EmptyQueryResponse) => transition!(Finished(0)),
                Some(_) => return Err(Error::unexpected_message()),
                None => return Err(Error::closed()),
            }
        }
    }
}

impl<C> ExecuteFuture<C>
where C: Channel<Request>,
{
    pub fn new(client: Client<C>, request: PendingRequest, statement: Statement) -> ExecuteFuture<C> {
        Execute::start(client, request, statement)
    }
}
