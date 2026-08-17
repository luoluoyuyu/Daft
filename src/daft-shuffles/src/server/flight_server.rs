use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::writer::IpcWriteOptions;
use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use daft_core::prelude::SchemaRef;
use daft_io::{GetResult, SourceType, get_io_client, parse_url};
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use tokio::{io::BufReader, sync::Mutex};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::{FlightDataStreamReader, ObjectStreamReader};
use crate::{
    client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream,
    shuffle_cache::ShuffleCache,
};

struct ParsedTicket {
    shuffle_id: u64,
    partition_idx: usize,
    cache_ids: Option<Vec<u32>>,
}

impl ParsedTicket {
    fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Ticket format: "shuffle_id:partition_idx:cache_ids" where cache_ids is comma-separated list of u32s
        let parts: Vec<&str> = ticket_str.splitn(3, ':').collect();
        if parts.len() < 2 {
            return Err(Status::invalid_argument(
                "Invalid ticket format. Expected 'shuffle_id:partition_idx' or 'shuffle_id:partition_idx:cache_ids'",
            ));
        }

        let shuffle_id = parts[0]
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("Invalid shuffle id: {}", e)))?;
        let partition_idx = parts[1]
            .parse::<usize>()
            .map_err(|e| Status::invalid_argument(format!("Invalid partition index: {}", e)))?;

        // Parse cache_ids if provided (third part of ticket)
        let cache_ids: Option<Vec<u32>> = if parts.len() == 3 && !parts[2].is_empty() {
            let ids = parts[2]
                .split(',')
                .map(|id| id.parse::<u32>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Status::invalid_argument(format!("Invalid cache id: {}", e)))?;
            Some(ids)
        } else {
            None
        };

        Ok(Self {
            shuffle_id,
            partition_idx,
            cache_ids,
        })
    }
}

#[derive(Clone, Default)]
pub struct ShuffleFlightServer {
    shuffle_caches: Arc<Mutex<HashMap<u64, Vec<Arc<ShuffleCache>>>>>,
    io_config: Option<Arc<daft_io::IOConfig>>,
}

impl ShuffleFlightServer {
    pub fn new() -> Self {
        Self::with_io_config(None)
    }

    /// Create a server with an optional IO config for reading object-store
    /// (S3/GCS/...) shuffle files directly from any executor.
    pub fn with_io_config(io_config: Option<Arc<daft_io::IOConfig>>) -> Self {
        Self {
            shuffle_caches: Arc::new(Mutex::new(HashMap::new())),
            io_config,
        }
    }

    /// Open a shuffle partition file (local path or object-store URI) and
    /// return a stream of raw Flight data frames.
    async fn open_flight_data_stream(
        &self,
        file_path: String,
    ) -> DaftResult<BoxStream<'static, DaftResult<FlightData>>> {
        let (source_type, _) = parse_url(&file_path)?;
        match source_type {
            SourceType::File => {
                let file_path = std::path::PathBuf::from(&file_path);
                let file = tokio::fs::File::open(&file_path)
                    .await
                    .map_err(DaftError::IoError)?;
                let reader = FlightDataStreamReader::try_new(BufReader::new(file)).await?;
                Ok(reader.into_stream().boxed())
            }
            _ => {
                let io_config = self.io_config.clone().ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "IO config required to read object-store shuffle file: {file_path}"
                    ))
                })?;
                let io_client = get_io_client(true, io_config)?;
                let (source, object_path) = io_client
                    .get_source_and_path(&file_path)
                    .await
                    .map_err(|e| DaftError::External(e.into()))?;
                let get_result = source
                    .get(&object_path, None, None)
                    .await
                    .map_err(|e| DaftError::External(e.into()))?;
                let stream = match get_result {
                    GetResult::Stream(byte_stream, _, _, _) => byte_stream,
                    GetResult::File(_) => {
                        return Err(DaftError::InternalError(format!(
                            "Expected a byte stream for object-store shuffle file {file_path}"
                        )));
                    }
                };
                let reader = FlightDataStreamReader::try_new(ObjectStreamReader::new(stream)).await?;
                Ok(reader.into_stream().boxed())
            }
        }
    }

    pub async fn register_shuffle_cache(
        &self,
        shuffle_id: u64,
        cache: Arc<ShuffleCache>,
    ) -> DaftResult<()> {
        let mut caches = self.shuffle_caches.lock().await;
        caches.entry(shuffle_id).or_insert(Vec::new()).push(cache);
        Ok(())
    }

    /// Return the numeric cache ids registered for a shuffle on this server.
    ///
    /// Used by the distributed executor when reporting a completed
    /// intermediate task: the scheduler stores these ids (together with the
    /// server's Flight address) as the upstream locations for the shuffle, so
    /// downstream tasks know which caches to read from.
    pub async fn cache_ids(&self, shuffle_id: u64) -> Vec<u32> {
        let caches = self.shuffle_caches.lock().await;
        caches
            .get(&shuffle_id)
            .map(|caches| {
                caches
                    .iter()
                    .filter_map(|cache| cache.cache_id().parse::<u32>().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drop the caches registered for a shuffle and remove their on-disk
    /// partition files. An empty ``cache_ids`` purges every cache of the
    /// shuffle. Called by the distributed executor when the scheduler tells
    /// it a completed job's shuffle is no longer needed.
    pub async fn purge_shuffle_caches(
        &self,
        shuffle_id: u64,
        cache_ids: &[u32],
    ) -> DaftResult<()> {
        let removed = {
            let mut caches = self.shuffle_caches.lock().await;
            let Some(shuffle_caches) = caches.get_mut(&shuffle_id) else {
                return Ok(());
            };
            let mut remaining = Vec::new();
            let mut removed = Vec::new();
            for cache in shuffle_caches.drain(..) {
                let matches = cache_ids.is_empty()
                    || cache
                        .cache_id()
                        .parse::<u32>()
                        .map(|id| cache_ids.contains(&id))
                        .unwrap_or(false);
                if matches {
                    removed.push(cache);
                } else {
                    remaining.push(cache);
                }
            }
            *shuffle_caches = remaining;
            if shuffle_caches.is_empty() {
                caches.remove(&shuffle_id);
            }
            removed
        };
        // Remove the spill files off the lock: this is blocking I/O.
        for cache in removed {
            cache.cleanup_files().await;
        }
        Ok(())
    }

    async fn get_shuffle_file_paths(
        &self,
        shuffle_id: u64,
        partition_idx: usize,
        cache_ids: Option<&[u32]>,
    ) -> Option<(Vec<String>, SchemaRef)> {
        let caches = self.shuffle_caches.lock().await;
        let shuffle_caches = caches.get(&shuffle_id).cloned()?;

        let schema = shuffle_caches
            .first()
            .expect("Expected at least one cache")
            .schema();

        // Filter caches by cache_ids if provided
        let filtered_caches: Vec<_> = if let Some(ids) = cache_ids {
            shuffle_caches
                .iter()
                .filter(|cache| {
                    // Parse cache_id from cache and check if it's in the requested ids
                    cache
                        .cache_id()
                        .parse::<u32>()
                        .map(|cache_id| ids.contains(&cache_id))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            shuffle_caches
        };

        let file_paths = filtered_caches
            .iter()
            .flat_map(|cache| cache.file_paths_for_partition(partition_idx))
            .collect::<Vec<_>>();
        if file_paths.is_empty() {
            eprintln!(
                "[shuffle-server] shuffle {shuffle_id} partition {partition_idx}: no files (empty partition)"
            );
        }

        Some((file_paths, schema))
    }

    /// Get partition data in-process (no gRPC). Returns a stream of Daft RecordBatches.
    /// Used when the reader runs on the same node as the shuffle server.
    pub async fn get_partition_local(
        &self,
        shuffle_id: u64,
        partition_idx: usize,
        cache_ids: &[u32],
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let (file_paths, schema) = self
            .get_shuffle_file_paths(shuffle_id, partition_idx, Some(cache_ids))
            .await
            .ok_or_else(|| {
                DaftError::ValueError(format!("Shuffle cache not found for id: {}", shuffle_id))
            })?;

        let this = self.clone();
        let file_path_stream = futures::stream::iter(file_paths);
        let flight_data_stream = file_path_stream
            .then(move |file_path| {
                let this = this.clone();
                let schema = schema.clone();
                async move {
                    let reader = this
                        .open_flight_data_stream(file_path)
                        .await?
                        .map_err(|e| FlightError::from_external_error(Box::new(e)));

                    let arrow_schema = schema.to_arrow().map_err(|e| {
                        DaftError::InternalError(format!("Error converting schema to arrow: {}", e))
                    })?;
                    let options = IpcWriteOptions::default();
                    let flight_schema = SchemaAsIpc::new(&arrow_schema, &options).into();
                    let flight_data =
                        futures::stream::once(async { Ok(flight_schema) }).chain(reader);

                    // Doing some shenanigans here to reuse existing code
                    // TODO: Refactor this to get Arrow RecordBatchStream directly using async IO
                    let arrow_stream = FlightRecordBatchStream::new_from_flight_data(flight_data);
                    let daft_stream =
                        FlightRecordBatchStreamToDaftRecordBatchStream::new(arrow_stream, schema);
                    Ok::<_, DaftError>(daft_stream)
                }
            })
            .try_flatten();

        Ok(Box::pin(flight_data_stream))
    }
}

#[tonic::async_trait]
impl FlightService for ShuffleFlightServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!("Handshake is not supported for shuffle server")
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!("List flights is not supported for shuffle server")
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        unimplemented!("Get flight info is not supported for shuffle server")
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented!("Poll flight info is not supported for shuffle server")
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented!("Get schema is not supported for shuffle server")
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let ticket = ParsedTicket::from_ticket(&ticket)?;

        let (file_paths, schema) = self
            .get_shuffle_file_paths(
                ticket.shuffle_id,
                ticket.partition_idx,
                ticket.cache_ids.as_deref(),
            )
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Shuffle cache not found for id: {}",
                    ticket.shuffle_id
                ))
            })?;

        let this = self.clone();
        let file_path_stream = futures::stream::iter(file_paths);
        let flight_data_stream = file_path_stream
            .then(move |file_path| {
                let this = this.clone();
                async move {
                    let stream = this
                        .open_flight_data_stream(file_path)
                        .await
                        .map_err(|e| Status::internal(format!("Error opening shuffle file: {e}")))?;
                    Ok::<_, Status>(stream.map_err(|e| Status::internal(e.to_string())))
                }
            })
            .try_flatten();

        let options = IpcWriteOptions::default();
        let arrow_schema = schema
            .to_arrow()
            .map_err(|e| Status::internal(format!("Error converting schema to arrow: {}", e)))?;
        let flight_schema = SchemaAsIpc::new(&arrow_schema, &options).into();
        let flight_data =
            futures::stream::once(async { Ok(flight_schema) }).chain(flight_data_stream);

        Ok(Response::new(Box::pin(flight_data)))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!("Do put is not supported for shuffle server")
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!("Do exchange is not supported for shuffle server")
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented!("Do action is not supported for shuffle server")
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!("List actions is not supported for shuffle server")
    }
}

pub struct FlightServerConnectionHandle {
    ip: String,
    port: u16,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    server_task: Option<RuntimeTask<DaftResult<()>>>,
}

impl FlightServerConnectionHandle {
    pub fn shutdown(&mut self) -> DaftResult<()> {
        let Some(shutdown_signal) = self.shutdown_signal.take() else {
            return Ok(());
        };
        let _ = shutdown_signal.send(());
        let Some(server_task) = self.server_task.take() else {
            return Ok(());
        };
        common_runtime::get_io_runtime(true).block_on_current_thread(server_task)??;
        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn shuffle_address(&self) -> String {
        format!("grpc://{}:{}", self.ip, self.port)
    }
}

pub fn start_server_loop(
    ip: &str,
    server: Arc<ShuffleFlightServer>,
) -> FlightServerConnectionHandle {
    let io_runtime = common_runtime::get_io_runtime(true);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (port_tx, port_rx) = tokio::sync::oneshot::channel();

    let addr = format!("{}:0", ip);
    let server_task = io_runtime.spawn(async {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("Failed to bind to port");

        let port = listener
            .local_addr()
            .expect("Failed to get local address")
            .port();

        port_tx.send(port).expect("Failed to send port");

        let incoming = tonic::transport::server::TcpIncoming::from(listener)
            .with_nodelay(Some(true))
            .with_keepalive(None);

        let flight_server = server;
        Server::builder()
            .add_service(FlightServiceServer::from_arc(flight_server))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| DaftError::InternalError(format!("Error serving flight server: {}", e)))?;

        Ok(())
    });

    let port = port_rx.blocking_recv().expect("Failed to receive port");

    FlightServerConnectionHandle {
        ip: ip.to_string(),
        port,
        shutdown_signal: Some(shutdown_tx),
        server_task: Some(server_task),
    }
}
