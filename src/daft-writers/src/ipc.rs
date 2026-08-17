use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    AsyncFileWriter, RETURN_PATHS_COLUMN_NAME, WriteResult, WriterFactory,
    storage_backend::{FileStorageBackend, StorageBackend},
};

/// Arrow IPC stream writer backed by a [`StorageBackend`] so shuffle spill
/// files can be written to local disk or object storage (e.g. S3) uniformly.
pub struct IPCWriter<B: StorageBackend> {
    is_closed: bool,
    bytes_written: usize,
    file_path: PathBuf,
    file_path_uri: String,
    compression: Option<arrow_ipc::CompressionType>,
    storage_backend: B,
    writer: Option<arrow_ipc::writer::StreamWriter<B::Writer>>,
}

impl<B: StorageBackend> IPCWriter<B> {
    pub fn new(
        file_path: &str,
        file_path_uri: String,
        compression: Option<arrow_ipc::CompressionType>,
        storage_backend: B,
    ) -> Self {
        Self {
            is_closed: false,
            bytes_written: 0,
            file_path: PathBuf::from(file_path),
            file_path_uri,
            compression,
            storage_backend,
            writer: None,
        }
    }

    async fn get_or_create_writer(
        &mut self,
        schema: &Schema,
    ) -> DaftResult<&mut arrow_ipc::writer::StreamWriter<B::Writer>> {
        if self.writer.is_none() {
            let file = self.storage_backend.create_writer(&self.file_path).await?;

            let arrow_schema = schema.to_arrow()?;
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(self.compression)?;

            let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                file,
                &arrow_schema,
                write_options,
            )?;
            self.writer = Some(writer);
        }
        Ok(self.writer.as_mut().unwrap())
    }
}

#[async_trait]
impl<B: StorageBackend> AsyncFileWriter for IPCWriter<B> {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        assert!(!self.is_closed, "Writer is closed");

        let size_bytes = data.size_bytes();
        let rows_written = data.len();
        let writer = self.get_or_create_writer(&data.schema()).await?;

        // Write each record batch
        for table in data.record_batches() {
            // Convert daft RecordBatch to arrow-rs RecordBatch
            let arrow_batch: arrow_array::RecordBatch = table.clone().try_into()?;
            writer.write(&arrow_batch)?;
        }

        // Track bytes written (approximate, since we can't easily get exact bytes from arrow-ipc)
        self.bytes_written += size_bytes;
        Ok(WriteResult {
            bytes_written: size_bytes,
            rows_written,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(mut writer) = self.writer.take() {
            writer.finish()?;
        }
        // Flush any remaining buffer and await multipart upload completion.
        self.storage_backend.finalize().await?;
        let path_col = Series::from_arrow(
            Arc::new(Field::new(RETURN_PATHS_COLUMN_NAME, DataType::Utf8)),
            Arc::new(arrow_array::LargeStringArray::from_iter_values(
                std::iter::once(self.file_path_uri.clone()),
            )),
        )?;
        let res = RecordBatch::from_nonempty_columns(vec![path_col])?;
        Ok(Some(res))
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written]
    }
}

pub struct IPCWriterFactory<B: StorageBackend> {
    dir: String,
    dir_uri: String,
    scheme: Option<String>,
    compression: Option<arrow_ipc::CompressionType>,
    make_backend: Box<dyn Fn() -> B + Send + Sync>,
}

impl<B: StorageBackend + 'static> IPCWriterFactory<B> {
    /// ``dir`` is the storage-agnostic path handed to the backend
    /// (``/local/path`` or ``bucket/key``); ``dir_uri`` is the full reporting
    /// URI (``/local/path`` or ``s3://bucket/key``); ``scheme`` is ``Some``
    /// when the files live on an object store.
    pub fn new<F: Fn() -> B + Send + Sync + 'static>(
        dir: String,
        dir_uri: String,
        scheme: Option<String>,
        compression: Option<arrow_ipc::CompressionType>,
        make_backend: F,
    ) -> Self {
        Self {
            dir,
            dir_uri,
            scheme,
            compression,
            make_backend: Box::new(make_backend),
        }
    }
}

impl<B: StorageBackend + 'static> WriterFactory for IPCWriterFactory<B> {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let file_path = format!("{}/{}.arrow", self.dir, file_idx);
        let file_path_uri = match &self.scheme {
            Some(scheme) => format!("{}://{}/{}.arrow", scheme, self.dir_uri, file_idx),
            None => format!("{}/{}.arrow", self.dir_uri, file_idx),
        };
        let writer = IPCWriter::new(
            &file_path,
            file_path_uri,
            self.compression,
            (self.make_backend)(),
        );
        Ok(Box::new(writer))
    }
}

/// Convenience alias for the storage-agnostic IPC writer factory used by the
/// shuffle cache. The concrete backend type is hidden behind the trait
/// object so callers can construct it from a plain URI.
pub type BoxedIPCWriterFactory =
    Box<dyn WriterFactory<Input = MicroPartition, Result = Option<RecordBatch>>>;

/// Build a storage-agnostic IPC writer factory from a URI.
///
/// ``dir_uri`` may be a plain local path or an object-store URI such as
/// ``s3://bucket/path``. ``io_config`` is required for object-store backends
/// (the same config used for regular Parquet writes).
pub fn make_boxed_ipc_writer_factory(
    dir_uri: &str,
    compression: Option<arrow_ipc::CompressionType>,
    io_config: Option<daft_io::IOConfig>,
) -> DaftResult<BoxedIPCWriterFactory> {
    use daft_io::{SourceType, parse_url};

    let (source_type, path) = parse_url(dir_uri)?;
    match source_type {
        SourceType::File => {
            let dir = path.to_string();
            let factory = IPCWriterFactory::<FileStorageBackend>::new(
                dir.clone(),
                dir.clone(),
                None,
                compression,
                || FileStorageBackend {},
            );
            Ok(Box::new(factory))
        }
        source if source.supports_native_writer() => {
            let scheme = daft_io::utils::parse_object_url(dir_uri)?.scheme;
            let io_config = io_config.ok_or_else(|| {
                common_error::DaftError::InternalError(
                    "IO config is required for object-store shuffle writes".to_string(),
                )
            })?;
            let dir = path.to_string();
            let factory = IPCWriterFactory::<crate::storage_backend::ObjectStorageBackend>::new(
                dir.clone(),
                dir.clone(),
                Some(scheme.clone()),
                compression,
                move || {
                    crate::storage_backend::ObjectStorageBackend::new(
                        scheme.clone(),
                        io_config.clone(),
                    )
                },
            );
            Ok(Box::new(factory))
        }
        _ => Err(common_error::DaftError::InternalError(format!(
            "Unsupported shuffle storage scheme: {source_type}"
        ))),
    }
}
