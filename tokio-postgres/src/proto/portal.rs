use std::sync::Arc;

use crate::proto::client::WeakClient;
use crate::proto::statement::Statement;
use crate::proto::Request;
use crate::Channel;

struct Inner<C>
where
    C: Channel<Request>,
{
    client: WeakClient<C>,
    name: String,
    statement: Statement,
}

impl<C> Drop for Inner<C>
where
    C: Channel<Request>,
{
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client.close_portal(&self.name);
        }
    }
}

#[derive(Clone)]
pub struct Portal<C>(Arc<Inner<C>>)
where
    C: Channel<Request>,
;

impl<C> Portal<C>
where
    C: Channel<Request>,
{
    pub fn new(client: WeakClient<C>, name: String, statement: Statement) -> Portal<C> {
        Portal(Arc::new(Inner {
            client,
            name,
            statement,
        }))
    }

    pub fn name(&self) -> &str {
        &self.0.name
    }

    pub fn statement(&self) -> &Statement {
        &self.0.statement
    }
}
