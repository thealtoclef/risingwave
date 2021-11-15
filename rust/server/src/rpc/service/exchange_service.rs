use crate::stream::StreamManager;
use crate::stream_op::Message;
use crate::task::{TaskManager, TaskSinkId};
use futures::channel::mpsc::Receiver;
use futures::StreamExt;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::data::StreamMessage;
use risingwave_pb::task_service::exchange_service_server::ExchangeService;
use risingwave_pb::task_service::{
    GetDataRequest, GetDataResponse, GetStreamRequest, TaskSinkId as ProtoTaskSinkId,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    mgr: Arc<TaskManager>,
    stream_mgr: Arc<StreamManager>,
}

impl ExchangeServiceImpl {
    pub fn new(mgr: Arc<TaskManager>, stream_mgr: Arc<StreamManager>) -> Self {
        ExchangeServiceImpl { mgr, stream_mgr }
    }
}

type ExchangeDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;
type ExchangeDataSender = tokio::sync::mpsc::Sender<std::result::Result<GetDataResponse, Status>>;

#[async_trait::async_trait]
impl ExchangeService for ExchangeServiceImpl {
    type GetDataStream = ExchangeDataStream;
    type GetStreamStream = ReceiverStream<std::result::Result<StreamMessage, Status>>;

    #[cfg(not(tarpaulin_include))]
    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let req = request.into_inner();
        match self
            .get_data_impl(peer_addr, req.get_sink_id().clone())
            .await
        {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!("Failed to serve exchange RPC from {}: {}", peer_addr, e);
                Err(e.to_grpc_status())
            }
        }
    }

    async fn get_stream(
        &self,
        request: Request<GetStreamRequest>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;
        let req = request.into_inner();
        let up_down_ids = (req.up_fragment_id, req.down_fragment_id);
        let receiver = self
            .stream_mgr
            .take_receiver(up_down_ids)
            .map_err(|e| e.to_grpc_status())?;
        match self.get_stream_impl(peer_addr, receiver).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!(
                    "Failed to server stream exchange RPC from {}: {}",
                    peer_addr, e
                );
                Err(e.to_grpc_status())
            }
        }
    }
}

impl ExchangeServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn get_data_impl(
        &self,
        peer_addr: SocketAddr,
        pb_tsid: ProtoTaskSinkId,
    ) -> Result<Response<<Self as ExchangeService>::GetDataStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let tsid = TaskSinkId::from(&pb_tsid);
        debug!("Serve exchange RPC from {} [{:?}]", peer_addr, tsid);
        let mut task_sink = self.mgr.take_sink(&pb_tsid)?;
        tokio::spawn(async move {
            let mut writer = GrpcExchangeWriter::new(tx.clone());
            match task_sink.take_data(&mut writer).await {
                Ok(_) => {
                    info!(
                        "Exchanged {} chunks from sink {:?}",
                        writer.written_chunks(),
                        tsid,
                    );
                    Ok(())
                }
                Err(e) => tx.send(Err(e.to_grpc_status())).await,
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream_impl(
        &self,
        peer_addr: SocketAddr,
        mut receiver: Receiver<Message>,
    ) -> Result<Response<<Self as ExchangeService>::GetStreamStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        debug!("Serve stream exchange RPC from {}", peer_addr);
        tokio::spawn(async move {
            loop {
                let msg = receiver.next().await;
                match msg {
                    // the sender is closed, we close the receiver and stop forwarding message
                    None => break,
                    Some(msg) => {
                        let res = match msg.to_protobuf() {
                            Ok(stream_msg) => Ok(stream_msg),
                            Err(e) => Err(e.to_grpc_status()),
                        };
                        let _ = match tx.send(res).await.map_err(|e| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "failed to send stream data: {}",
                                e
                            )))
                        }) {
                            Ok(_) => Ok(()),
                            Err(e) => tx.send(Err(e.to_grpc_status())).await,
                        };
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[async_trait::async_trait]
pub trait ExchangeWriter: Send {
    async fn write(&mut self, resp: GetDataResponse) -> Result<()>;
}

pub struct GrpcExchangeWriter {
    sender: ExchangeDataSender,
    written_chunks: usize,
}

impl GrpcExchangeWriter {
    fn new(sender: ExchangeDataSender) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

#[async_trait::async_trait]
impl ExchangeWriter for GrpcExchangeWriter {
    async fn write(&mut self, data: GetDataResponse) -> Result<()> {
        self.written_chunks += 1;
        self.sender.send(Ok(data)).await.map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "failed to write data to ExchangeWriter: {}",
                e
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc::service::exchange_service::{ExchangeWriter, GrpcExchangeWriter};
    use risingwave_pb::task_service::GetDataResponse;

    #[tokio::test]
    async fn test_exchange_writer() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let mut writer = GrpcExchangeWriter::new(tx);
        writer.write(GetDataResponse::default()).await.unwrap();
        assert_eq!(writer.written_chunks(), 1);
    }

    #[tokio::test]
    async fn test_write_to_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        drop(rx);
        let mut writer = GrpcExchangeWriter::new(tx);
        let res = writer.write(GetDataResponse::default()).await;
        assert!(res.is_err());
    }
}
