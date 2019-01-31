use futures::sync::mpsc;
use futures::{Poll, Sink, StartSend};
use std::marker::PhantomData;

use crate::proto::connection::Request;
use crate::types::ToSql;
use crate::{Client, Statement};

pub struct ExecuteSink<T, F>
where
    F: Fn(&T) -> Vec<&dyn ToSql>,
{
    statement: Statement,
    client: Client,
    sender: mpsc::Sender<Request>,
    to_sql: F,
    phantom_data: PhantomData<T>,
}

impl<T, F> Sink for ExecuteSink<T, F>
where
    F: Fn(&T) -> Vec<&dyn ToSql>,
{
    type SinkItem = T;
    type SinkError = crate::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let params = (self.to_sql)(&item);
        self.client.0.excecute_message(&self.statement.0, params.as_ref())?;
        unimplemented!();
        Ok(futures::AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        unimplemented!()
    }
}
