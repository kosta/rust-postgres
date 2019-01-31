use futures::{Poll, Sink, StartSend};
use std::marker::PhantomData;

use crate::{Client, Statement};
use crate::types::ToSql;

pub struct ExecuteSink<T, F>
    where
        F: Fn(&T) -> &[&dyn ToSql]
{
    statement: Statement,
    client: Client,
    as_sql: F,
    phantom_data: PhantomData<T>,
}

impl<T, F> Sink for ExecuteSink<T, F>
where F: Fn(&T) -> &[&dyn ToSql]
{
    type SinkItem = T;
    type SinkError = crate::Error;

    fn start_send(
    &mut self,
    item: Self::SinkItem
) -> StartSend<Self::SinkItem, Self::SinkError> {
    unimplemented!()
}

fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
    unimplemented!()
}
}