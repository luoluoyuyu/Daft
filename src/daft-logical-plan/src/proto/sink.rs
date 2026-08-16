use super::*;

use common_file_formats::{FileFormat, WriteMode};

pub(crate) fn file_format_to_proto(format: FileFormat) -> proto::FileFormat {
    use proto::FileFormat as P;
    match format {
        FileFormat::Parquet => P::Parquet,
        FileFormat::Csv => P::Csv,
        FileFormat::Json => P::Json,
        FileFormat::Warc => P::Warc,
        FileFormat::Text => P::Text,
    }
}

pub(crate) fn file_format_from_proto(format: proto::FileFormat) -> DaftResult<FileFormat> {
    use proto::FileFormat as P;
    match format {
        P::Parquet => Ok(FileFormat::Parquet),
        P::Csv => Ok(FileFormat::Csv),
        P::Json => Ok(FileFormat::Json),
        P::Warc => Ok(FileFormat::Warc),
        P::Text => Ok(FileFormat::Text),
        P::Unspecified => invalid("FileFormat::Unspecified"),
    }
}

pub(crate) fn write_mode_to_proto(mode: WriteMode) -> proto::WriteMode {
    use proto::WriteMode as P;
    match mode {
        WriteMode::Overwrite => P::Overwrite,
        WriteMode::OverwritePartitions => P::OverwritePartitions,
        WriteMode::Append => P::Append,
    }
}

pub(crate) fn write_mode_from_proto(mode: proto::WriteMode) -> DaftResult<WriteMode> {
    use proto::WriteMode as P;
    match mode {
        P::Overwrite => Ok(WriteMode::Overwrite),
        P::OverwritePartitions => Ok(WriteMode::OverwritePartitions),
        P::Append => Ok(WriteMode::Append),
        P::Unspecified => invalid("WriteMode::Unspecified"),
    }
}

pub(crate) fn format_sink_option_to_proto(
    option: &crate::sink_info::FormatSinkOption,
) -> proto::FormatSinkOption {
    use crate::sink_info::{CsvFormatOption, FormatSinkOption, JsonFormatOption};
    use proto::format_sink_option::Format;
    let format = match option {
        FormatSinkOption::Csv(csv) => Format::Csv(proto::CsvFormatOption {
            delimiter: csv.delimiter.map(|b| b as u32),
            quote: csv.quote.map(|b| b as u32),
            escape: csv.escape.map(|b| b as u32),
            header: csv.header,
            date_format: csv.date_format.clone(),
            timestamp_format: csv.timestamp_format.clone(),
        }),
        FormatSinkOption::Json(json) => Format::Json(proto::JsonFormatOption {
            ignore_null_fields: json.ignore_null_fields,
            date_format: json.date_format.clone(),
            timestamp_format: json.timestamp_format.clone(),
        }),
        FormatSinkOption::Parquet(_) => Format::Parquet(true),
    };
    proto::FormatSinkOption {
        format: Some(format),
    }
}

pub(crate) fn format_sink_option_from_proto(
    option: proto::FormatSinkOption,
) -> DaftResult<crate::sink_info::FormatSinkOption> {
    use crate::sink_info::{CsvFormatOption, FormatSinkOption, JsonFormatOption};
    use proto::format_sink_option::Format;
    match required(option.format, "FormatSinkOption.format")? {
        Format::Csv(csv) => Ok(FormatSinkOption::Csv(CsvFormatOption {
            delimiter: csv.delimiter.map(|b| b as u8),
            quote: csv.quote.map(|b| b as u8),
            escape: csv.escape.map(|b| b as u8),
            header: csv.header,
            date_format: csv.date_format,
            timestamp_format: csv.timestamp_format,
        })),
        Format::Json(json) => Ok(FormatSinkOption::Json(JsonFormatOption {
            ignore_null_fields: json.ignore_null_fields,
            date_format: json.date_format,
            timestamp_format: json.timestamp_format,
        })),
        Format::Parquet(_) => Ok(FormatSinkOption::Parquet(
            crate::sink_info::ParquetFormatOption {},
        )),
    }
}

pub(crate) fn output_file_info_to_proto(
    info: &crate::sink_info::OutputFileInfo,
) -> DaftResult<proto::OutputFileInfo> {
    Ok(proto::OutputFileInfo {
        root_dir: info.root_dir.clone(),
        write_mode: write_mode_to_proto(info.write_mode) as i32,
        file_format: file_format_to_proto(info.file_format) as i32,
        format_option: info.format_option.as_ref().map(format_sink_option_to_proto),
        partition_cols: match &info.partition_cols {
            Some(cols) => Some(proto::ExpressionList {
                items: cols
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<Vec<_>>>()?,
            }),
            None => None,
        },
        compression: info.compression.clone(),
        io_config: opt_bincode_to_bytes(&info.io_config, "OutputFileInfo.io_config"),
        write_success_file: info.write_success_file,
    })
}

pub(crate) fn output_file_info_from_proto(
    info: proto::OutputFileInfo,
) -> DaftResult<crate::sink_info::OutputFileInfo> {
    Ok(crate::sink_info::OutputFileInfo {
        root_dir: info.root_dir,
        write_mode: write_mode_from_proto(
            proto::WriteMode::try_from(info.write_mode)
                .map_err(|_| DaftError::ValueError("invalid WriteMode".to_string()))?,
        )?,
        file_format: file_format_from_proto(
            proto::FileFormat::try_from(info.file_format)
                .map_err(|_| DaftError::ValueError("invalid FileFormat".to_string()))?,
        )?,
        format_option: info.format_option.map(format_sink_option_from_proto).transpose()?,
        partition_cols: match info.partition_cols {
            Some(list) => Some(
                list.items
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<Vec<_>>>()?,
            ),
            None => None,
        },
        compression: info.compression,
        io_config: opt_bincode_from_bytes(&info.io_config, "OutputFileInfo.io_config")?,
        write_success_file: info.write_success_file,
    })
}

pub(crate) fn sink_info_to_proto(info: &crate::sink_info::SinkInfo) -> DaftResult<proto::SinkInfo> {
    use crate::sink_info::SinkInfo;
    use proto::sink_info::Sink;
    let sink = match info {
        SinkInfo::OutputFileInfo(output) => Sink::OutputFile(output_file_info_to_proto(output)?),
        #[cfg(feature = "python")]
        SinkInfo::CatalogInfo(catalog) => Sink::Catalog(
            bincode::serde::encode_to_vec(catalog, bincode::config::legacy()).map_err(|e| {
                DaftError::ValueError(format!("failed to bincode-encode CatalogInfo: {e}"))
            })?,
        ),
        #[cfg(feature = "python")]
        SinkInfo::DataSinkInfo(data_sink) => Sink::DataSink(
            bincode::serde::encode_to_vec(data_sink, bincode::config::legacy()).map_err(|e| {
                DaftError::ValueError(format!("failed to bincode-encode DataSinkInfo: {e}"))
            })?,
        ),
    };
    Ok(proto::SinkInfo { sink: Some(sink) })
}

pub(crate) fn sink_info_from_proto(info: proto::SinkInfo) -> DaftResult<crate::sink_info::SinkInfo> {
    use crate::sink_info::SinkInfo;
    use proto::sink_info::Sink;
    match required(info.sink, "SinkInfo.sink")? {
        Sink::OutputFile(output) => {
            Ok(SinkInfo::OutputFileInfo(output_file_info_from_proto(output)?))
        }
        Sink::Catalog(bytes) => {
            #[cfg(feature = "python")]
            {
                let catalog = opt_bincode_from_bytes::<crate::sink_info::CatalogInfo>(
                    &bytes,
                    "SinkInfo.catalog",
                )?
                .ok_or_else(|| {
                    DaftError::ValueError("SinkInfo.catalog payload is empty".to_string())
                })?;
                Ok(SinkInfo::CatalogInfo(catalog))
            }
            #[cfg(not(feature = "python"))]
            {
                let _ = bytes;
                unsupported("SinkInfo::CatalogInfo requires a Python build")
            }
        }
        Sink::DataSink(bytes) => {
            #[cfg(feature = "python")]
            {
                let data_sink = opt_bincode_from_bytes::<crate::sink_info::DataSinkInfo>(
                    &bytes,
                    "SinkInfo.data_sink",
                )?
                .ok_or_else(|| {
                    DaftError::ValueError("SinkInfo.data_sink payload is empty".to_string())
                })?;
                Ok(SinkInfo::DataSinkInfo(data_sink))
            }
            #[cfg(not(feature = "python"))]
            {
                let _ = bytes;
                unsupported("SinkInfo::DataSinkInfo requires a Python build")
            }
        }
    }
}
