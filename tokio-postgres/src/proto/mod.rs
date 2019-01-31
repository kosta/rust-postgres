macro_rules! try_ready_receive {
    ($e:expr) => {
        match $e {
            Ok(::futures::Async::Ready(v)) => v,
            Ok(::futures::Async::NotReady) => return Ok(::futures::Async::NotReady),
            Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
        }
    };
}

macro_rules! try_ready_closed {
    ($e:expr) => {
        match $e {
            Ok(::futures::Async::Ready(v)) => v,
            Ok(::futures::Async::NotReady) => return Ok(::futures::Async::NotReady),
            Err(_) => return Err(crate::Error::closed()),
        }
    };
}

mod bind;
#[cfg(feature = "runtime")]
mod cancel_query;
mod cancel_query_raw;
mod client;
mod codec;
#[cfg(feature = "runtime")]
mod connect;
#[cfg(feature = "runtime")]
mod connect_once;
mod connect_raw;
#[cfg(feature = "runtime")]
mod connect_socket;
mod connection;
mod copy_in;
mod copy_out;
mod execute;
mod execute_sink;
mod idle;
mod maybe_tls_stream;
mod portal;
mod prepare;
mod query;
mod simple_query;
mod statement;
mod tls;
mod transaction;
mod typeinfo;
mod typeinfo_composite;
mod typeinfo_enum;

pub use crate::proto::bind::BindFuture;
#[cfg(feature = "runtime")]
pub use crate::proto::cancel_query::CancelQueryFuture;
pub use crate::proto::cancel_query_raw::CancelQueryRawFuture;
pub use crate::proto::client::Client;
pub use crate::proto::codec::PostgresCodec;
#[cfg(feature = "runtime")]
pub use crate::proto::connect::ConnectFuture;
#[cfg(feature = "runtime")]
pub use crate::proto::connect_once::ConnectOnceFuture;
pub use crate::proto::connect_raw::ConnectRawFuture;
#[cfg(feature = "runtime")]
pub use crate::proto::connect_socket::ConnectSocketFuture;
pub use crate::proto::connection::Connection;
pub use crate::proto::copy_in::CopyInFuture;
pub use crate::proto::copy_out::CopyOutStream;
pub use crate::proto::execute::ExecuteFuture;
pub use crate::proto::execute_sink::ExecuteSink;
pub use crate::proto::maybe_tls_stream::MaybeTlsStream;
pub use crate::proto::portal::Portal;
pub use crate::proto::prepare::PrepareFuture;
pub use crate::proto::query::QueryStream;
pub use crate::proto::simple_query::SimpleQueryStream;
pub use crate::proto::statement::Statement;
pub use crate::proto::tls::TlsFuture;
pub use crate::proto::transaction::TransactionFuture;
