use super::*;
pub(crate) fn interval_to_proto(interval: &IntervalValue) -> proto::IntervalValue {
    proto::IntervalValue {
        months: interval.months,
        days: interval.days,
        nanoseconds: interval.nanoseconds,
    }
}

pub(crate) fn interval_from_proto(interval: proto::IntervalValue) -> IntervalValue {
    IntervalValue::new(interval.months, interval.days, interval.nanoseconds)
}

pub(crate) fn file_reference_to_proto(reference: &FileReference) -> DaftResult<proto::FileReference> {
    Ok(proto::FileReference {
        media_type: media_type_to_proto(reference.media_type) as i32,
        url: reference.url.clone(),
        io_config: bincode::serde::encode_to_vec(&reference.io_config, bincode::config::legacy())
            .map_err(|e| {
                DaftError::ValueError(format!("failed to bincode-encode IOConfig: {e}"))
            })?,
    })
}

pub(crate) fn file_reference_from_proto(reference: proto::FileReference) -> DaftResult<FileReference> {
    let io_config =
        bincode::serde::decode_from_slice(&reference.io_config, bincode::config::legacy())
            .map_err(|e| DaftError::ValueError(format!("failed to bincode-decode IOConfig: {e}")))?
            .0;
    Ok(FileReference {
        media_type: media_type_from_proto(reference.media_type)?,
        url: reference.url,
        io_config,
    })
}

// ---------------------------------------------------------------------------
// Literal
// ---------------------------------------------------------------------------

pub trait LiteralProto {
    fn to_proto(&self) -> DaftResult<proto::Literal>;
}

pub trait ProtoLiteral {
    fn into_daft(self) -> DaftResult<Literal>;
}

impl LiteralProto for Literal {
    fn to_proto(&self) -> DaftResult<proto::Literal> {
        use proto::literal::Lit;
        let lit = match self {
            Literal::Null => Lit::Null(true),
            Literal::Boolean(value) => Lit::Boolean(*value),
            Literal::Utf8(value) => Lit::Utf8(value.clone()),
            Literal::Binary(value) => Lit::Binary(value.clone()),
            Literal::Int8(value) => Lit::Int8(*value as i32),
            Literal::UInt8(value) => Lit::Uint8(*value as u32),
            Literal::Int16(value) => Lit::Int16(*value as i32),
            Literal::UInt16(value) => Lit::Uint16(*value as u32),
            Literal::Int32(value) => Lit::Int32(*value),
            Literal::UInt32(value) => Lit::Uint32(*value),
            Literal::Int64(value) => Lit::Int64(*value),
            Literal::UInt64(value) => Lit::Uint64(*value),
            Literal::Timestamp(value, unit, timezone) => Lit::Timestamp(proto::TimestampLiteral {
                value: *value,
                time_unit: time_unit_to_proto(*unit) as i32,
                timezone: timezone.clone(),
            }),
            Literal::Date(value) => Lit::Date(*value),
            Literal::Time(value, unit) => Lit::Time(proto::TimeLiteral {
                value: *value,
                time_unit: time_unit_to_proto(*unit) as i32,
            }),
            Literal::Duration(value, unit) => Lit::Duration(proto::DurationLiteral {
                value: *value,
                time_unit: time_unit_to_proto(*unit) as i32,
            }),
            Literal::Interval(value) => Lit::Interval(interval_to_proto(value)),
            Literal::Float32(value) => Lit::Float32(*value),
            Literal::Float64(value) => Lit::Float64(*value),
            Literal::Decimal(value, precision, scale) => {
                let (low, high) = split_i128(*value);
                Lit::Decimal(proto::DecimalLiteral {
                    low,
                    high,
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            Literal::List(series) => Lit::List(series_to_ipc(series)?),
            #[cfg(feature = "python")]
            Literal::Python(_) => {
                return unsupported("Literal::Python on the pure-Rust serialization path");
            }
            Literal::Struct(values) => {
                let mut fields = proto::Literal {
                    r#struct: Default::default(),
                    lit: None,
                };
                for (key, value) in values {
                    fields.r#struct.insert(key.clone(), value.to_proto()?);
                }
                return Ok(fields);
            }
            Literal::File(reference) => Lit::File(file_reference_to_proto(reference)?),
            Literal::Tensor { data, shape } => Lit::Tensor(proto::TensorLiteral {
                data: series_to_ipc(data)?,
                shape: shape.clone(),
            }),
            Literal::SparseTensor {
                values,
                indices,
                shape,
                indices_offset,
            } => Lit::SparseTensor(proto::SparseTensorLiteral {
                values: series_to_ipc(values)?,
                indices: series_to_ipc(indices)?,
                shape: shape.clone(),
                indices_offset: *indices_offset,
            }),
            Literal::Embedding(series) => Lit::Embedding(series_to_ipc(series)?),
            Literal::Map { keys, values } => Lit::Map(proto::MapLiteral {
                keys: series_to_ipc(keys)?,
                values: series_to_ipc(values)?,
            }),
            Literal::Image(image) => {
                let payload = bincode::serde::encode_to_vec(image, bincode::config::legacy())
                    .map_err(|e| {
                        DaftError::ValueError(format!("failed to bincode-encode Image: {e}"))
                    })?;
                Lit::Image(payload)
            }
            Literal::Extension(series) => Lit::Extension(series_to_ipc(series)?),
        };
        Ok(proto::Literal {
            r#struct: Default::default(),
            lit: Some(lit),
        })
    }
}

impl ProtoLiteral for proto::Literal {
    fn into_daft(self) -> DaftResult<Literal> {
        use proto::literal::Lit;
        if self.lit.is_none() {
            // Empty oneof + no struct fields => empty struct literal.
            return Ok(Literal::Struct(IndexMap::new()));
        }
        let lit = self.lit.expect("checked above");
        let value = match lit {
            Lit::Null(_) => Literal::Null,
            Lit::Boolean(value) => Literal::Boolean(value),
            Lit::Utf8(value) => Literal::Utf8(value),
            Lit::Binary(value) => Literal::Binary(value),
            Lit::Int8(value) => Literal::Int8(value as i8),
            Lit::Uint8(value) => Literal::UInt8(value as u8),
            Lit::Int16(value) => Literal::Int16(value as i16),
            Lit::Uint16(value) => Literal::UInt16(value as u16),
            Lit::Int32(value) => Literal::Int32(value),
            Lit::Uint32(value) => Literal::UInt32(value),
            Lit::Int64(value) => Literal::Int64(value),
            Lit::Uint64(value) => Literal::UInt64(value),
            Lit::Timestamp(t) => Literal::Timestamp(
                t.value,
                time_unit_from_proto(t.time_unit)?,
                t.timezone,
            ),
            Lit::Date(value) => Literal::Date(value),
            Lit::Time(t) => Literal::Time(t.value, time_unit_from_proto(t.time_unit)?),
            Lit::Duration(d) => Literal::Duration(d.value, time_unit_from_proto(d.time_unit)?),
            Lit::Interval(interval) => Literal::Interval(interval_from_proto(interval)),
            Lit::Float32(value) => Literal::Float32(value),
            Lit::Float64(value) => Literal::Float64(value),
            Lit::Decimal(d) => Literal::Decimal(
                join_i128(d.low, d.high),
                u8::try_from(d.precision).unwrap_or(u8::MAX),
                d.scale as i8,
            ),
            Lit::List(bytes) => Literal::List(series_from_ipc(&bytes)?),
            Lit::Python(_) => {
                return unsupported("Literal::Python on the pure-Rust deserialization path");
            }
            Lit::File(reference) => Literal::File(file_reference_from_proto(reference)?),
            Lit::Tensor(t) => Literal::Tensor {
                data: series_from_ipc(&t.data)?,
                shape: t.shape,
            },
            Lit::SparseTensor(t) => Literal::SparseTensor {
                values: series_from_ipc(&t.values)?,
                indices: series_from_ipc(&t.indices)?,
                shape: t.shape,
                indices_offset: t.indices_offset,
            },
            Lit::Embedding(bytes) => Literal::Embedding(series_from_ipc(&bytes)?),
            Lit::Map(m) => Literal::Map {
                keys: series_from_ipc(&m.keys)?,
                values: series_from_ipc(&m.values)?,
            },
            Lit::Image(payload) => {
                let image =
                    bincode::serde::decode_from_slice(&payload, bincode::config::legacy())
                        .map_err(|e| {
                            DaftError::ValueError(format!("failed to bincode-decode Image: {e}"))
                        })?
                        .0;
                Literal::Image(image)
            }
            Lit::Extension(bytes) => Literal::Extension(series_from_ipc(&bytes)?),
        };
        if self.r#struct.is_empty() {
            Ok(value)
        } else {
            let mut fields = IndexMap::new();
            for (key, value) in self.r#struct {
                fields.insert(key, value.into_daft()?);
            }
            Ok(Literal::Struct(fields))
        }
    }
}

