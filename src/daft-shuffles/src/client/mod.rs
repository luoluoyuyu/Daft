pub mod flight_client;

use std::collections::HashMap;
use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::client::flight_client::ShuffleFlightClient;

pub struct FlightClientManager {
    clients: HashMap<String, ShuffleFlightClient>,
    limits: HashMap<String, Arc<Semaphore>>,
    retries: u64,
    max_concurrency_per_address: usize,
}

impl FlightClientManager {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            limits: HashMap::new(),
            retries: 0,
            max_concurrency_per_address: usize::MAX,
        }
    }

    pub fn with_options(retries: u64, max_concurrency_per_address: u64) -> Self {
        Self {
            retries,
            max_concurrency_per_address: if max_concurrency_per_address == 0 {
                Semaphore::MAX_PERMITS
            } else {
                usize::try_from(max_concurrency_per_address)
                    .unwrap_or(Semaphore::MAX_PERMITS)
                    .clamp(1, Semaphore::MAX_PERMITS)
            },
            ..Self::new()
        }
    }

    pub async fn fetch_partition(
        &mut self,
        shuffle_id: u64,
        partition: usize,
        server_cache_mapping: &HashMap<String, Vec<u32>>,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        // Ensure clients exist for all addresses before collecting futures
        for address in server_cache_mapping.keys() {
            self.clients
                .entry(address.clone())
                .or_insert_with(|| ShuffleFlightClient::new(address.clone()));
            self.limits.entry(address.clone()).or_insert_with(|| {
                Arc::new(Semaphore::new(self.max_concurrency_per_address))
            });
        }

        let mut futures = Vec::new();
        for (address, client) in &mut self.clients {
            if let Some(cache_ids) = server_cache_mapping.get(address) {
                let permit = self.limits[address].clone().acquire_owned().await.map_err(|e| {
                    common_error::DaftError::External(e.to_string().into())
                })?;
                let retries = self.retries;
                let stream_schema = schema.clone();
                futures.push(async move {
                    let stream = client.get_partition(
                        shuffle_id, partition, cache_ids.as_slice(), stream_schema, retries,
                    ).await?;
                    Ok::<_, common_error::DaftError>(stream.map(move |item| {
                        let _hold_permit: &OwnedSemaphorePermit = &permit;
                        item
                    }))
                });
            }
        }

        let remote_streams = futures::future::try_join_all(futures).await?;
        let record_batches =
            futures::stream::iter(remote_streams.into_iter()).flatten_unordered(None);
        Ok(record_batches.boxed())
    }
}

impl Default for FlightClientManager {
    fn default() -> Self {
        Self::new()
    }
}
