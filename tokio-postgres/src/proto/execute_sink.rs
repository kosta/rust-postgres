use futures::sync::mpsc;
use futures::{Poll, Sink, StartSend};
use std::marker::PhantomData;

use crate::proto::client::Client;
use crate::proto::connection::{Request, RequestMessages};
use crate::proto::idle::IdleState;
use crate::proto::statement::Statement;
use crate::types::ToSql;
use crate::Error;

pub struct ExecuteSink<T, F>
where
    // TODO: I'd love to make this -> AsRef<&[&dyn ToSql]>, but I don't know how
    F: Fn(&T) -> Vec<&dyn ToSql>,
{
    pub(crate) statement: Statement,
    pub(crate) client: Client,
    pub(crate) sender: mpsc::Sender<Request>,
    pub(crate) to_sql: F,
    pub(crate) idle: IdleState,
    pub(crate) phantom_data: PhantomData<T>,
}

impl<T, F> Sink for ExecuteSink<T, F>
where
    F: Fn(&T) -> Vec<&dyn ToSql>,
{
    type SinkItem = T;
    type SinkError = crate::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let params = (self.to_sql)(&item);
        let msg = self
            .client
            .excecute_message(&self.statement, params.as_ref())?;

        self.sender
            .start_send(Request {
                messages: RequestMessages::Single(msg),
                sender: None,
                idle: Some(self.idle.guard()),
            })
            .map(|ok| ok.map(|_| item))
            .map_err(|e| {
                eprintln!("ExecuteSink.start_send error: {:?}", e);
                Error::closed()
            })
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        eprintln!("ExecuteSink.poll_complete");
        self.sender.poll_complete().map_err(|e| {
            eprintln!("ExecuteSink.poll_complete error: {:?}", e);
            Error::closed()
        })
    }
}
