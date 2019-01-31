use std::sync::Arc;

use crate::proto::client::WeakClient;
use crate::types::Type;
use crate::proto::Request;
use crate::{Channel, Column};

pub struct StatementInner<C>
where
    C: Channel<Request>,
{
    client: WeakClient<C>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl<C> Drop for StatementInner<C>
where
    C: Channel<Request>,
{
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client.close_statement(&self.name);
        }
    }
}

#[derive(Clone)]
pub struct Statement<C>(Arc<StatementInner<C>>)
where
    C: Channel<Request>;

impl<C> Statement<C>
where
    C: Channel<Request>,
{
    pub fn new(
        client: WeakClient<C>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement<C> {
        Statement(Arc::new(StatementInner {
            client,
            name,
            params,
            columns,
        }))
    }

    pub fn name(&self) -> &str {
        &self.0.name
    }

    pub fn params(&self) -> &[Type] {
        &self.0.params
    }

    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}
