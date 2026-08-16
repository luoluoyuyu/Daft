use super::*;
pub trait DataTypeProto {
    fn to_proto(&self) -> DaftResult<proto::DataType>;
}

pub trait ProtoDataType {
    fn into_daft(self) -> DaftResult<DataType>;
}

impl DataTypeProto for DataType {
    fn to_proto(&self) -> DaftResult<proto::DataType> {
        use proto::data_type::Dt;
        let dt = match self {
            DataType::Null => Dt::Null(true),
            DataType::Boolean => Dt::Boolean(true),
            DataType::Int8 => Dt::Int8(true),
            DataType::Int16 => Dt::Int16(true),
            DataType::Int32 => Dt::Int32(true),
            DataType::Int64 => Dt::Int64(true),
            DataType::UInt8 => Dt::Uint8(true),
            DataType::UInt16 => Dt::Uint16(true),
            DataType::UInt32 => Dt::Uint32(true),
            DataType::UInt64 => Dt::Uint64(true),
            DataType::Float32 => Dt::Float32(true),
            DataType::Float64 => Dt::Float64(true),
            DataType::Decimal128(precision, scale) => Dt::Decimal128(proto::DecimalType {
                precision: *precision as u32,
                scale: *scale as u32,
            }),
            DataType::Timestamp(unit, timezone) => {
                Dt::Timestamp(proto::TimestampType {
                    time_unit: time_unit_to_proto(*unit) as i32,
                    timezone: timezone.clone(),
                })
            }
            DataType::Date => Dt::Date(true),
            DataType::Time(unit) => Dt::Time(time_unit_to_proto(*unit) as i32),
            DataType::Duration(unit) => Dt::Duration(time_unit_to_proto(*unit) as i32),
            DataType::Interval => Dt::Interval(true),
            DataType::Binary => Dt::Binary(true),
            DataType::FixedSizeBinary(size) => Dt::FixedSizeBinary(*size as u64),
            DataType::Utf8 => Dt::Utf8(true),
            DataType::FixedSizeList(dtype, size) => {
                Dt::FixedSizeList(Box::new(proto::FixedSizeListType {
                    dtype: Some(Box::new(dtype.to_proto()?)),
                    size: *size as u64,
                }))
            }
            DataType::List(dtype) => Dt::List(Box::new(dtype.to_proto()?)),
            DataType::Struct(fields) => Dt::Struct(proto::StructType {
                fields: fields.iter().map(field_to_proto).collect::<DaftResult<_>>()?,
            }),
            DataType::Map { key, value } => Dt::Map(Box::new(proto::MapType {
                key: Some(Box::new(key.to_proto()?)),
                value: Some(Box::new(value.to_proto()?)),
            })),
            DataType::Extension(name, dtype, metadata) => {
                Dt::Extension(Box::new(proto::ExtensionType {
                    name: name.clone(),
                    dtype: Some(Box::new(dtype.to_proto()?)),
                    metadata: metadata.clone(),
                }))
            }
            DataType::Embedding(dtype, size) => Dt::Embedding(Box::new(proto::EmbeddingType {
                dtype: Some(Box::new(dtype.to_proto()?)),
                size: *size as u64,
            })),
            DataType::Image(mode) => Dt::Image(proto::ImageType {
                mode: mode.map(|m| image_mode_to_proto(m) as i32),
            }),
            DataType::FixedShapeImage(mode, height, width) => {
                Dt::FixedShapeImage(proto::FixedShapeImageType {
                    mode: image_mode_to_proto(*mode) as i32,
                    height: *height,
                    width: *width,
                })
            }
            DataType::Tensor(dtype) => Dt::Tensor(Box::new(dtype.to_proto()?)),
            DataType::FixedShapeTensor(dtype, shape) => {
                Dt::FixedShapeTensor(Box::new(proto::FixedShapeTensorType {
                    dtype: Some(Box::new(dtype.to_proto()?)),
                    shape: shape.clone(),
                }))
            }
            DataType::SparseTensor(dtype, indices_offset) => {
                Dt::SparseTensor(Box::new(proto::SparseTensorType {
                    dtype: Some(Box::new(dtype.to_proto()?)),
                    indices_offset: *indices_offset,
                }))
            }
            DataType::FixedShapeSparseTensor(dtype, shape, indices_offset) => {
                Dt::FixedShapeSparseTensor(Box::new(proto::FixedShapeSparseTensorType {
                    dtype: Some(Box::new(dtype.to_proto()?)),
                    shape: shape.clone(),
                    indices_offset: *indices_offset,
                }))
            }
            #[cfg(feature = "python")]
            DataType::Python => Dt::Python(true),
            DataType::Unknown => Dt::Unknown(true),
            DataType::File(media_type) => Dt::File(media_type_to_proto(*media_type) as i32),
        };
        Ok(proto::DataType { dt: Some(dt) })
    }
}

impl ProtoDataType for proto::DataType {
    fn into_daft(self) -> DaftResult<DataType> {
        use proto::data_type::Dt;
        match self.dt {
            None => invalid("DataType without a oneof value"),
            Some(Dt::Null(_)) => Ok(DataType::Null),
            Some(Dt::Boolean(_)) => Ok(DataType::Boolean),
            Some(Dt::Int8(_)) => Ok(DataType::Int8),
            Some(Dt::Int16(_)) => Ok(DataType::Int16),
            Some(Dt::Int32(_)) => Ok(DataType::Int32),
            Some(Dt::Int64(_)) => Ok(DataType::Int64),
            Some(Dt::Uint8(_)) => Ok(DataType::UInt8),
            Some(Dt::Uint16(_)) => Ok(DataType::UInt16),
            Some(Dt::Uint32(_)) => Ok(DataType::UInt32),
            Some(Dt::Uint64(_)) => Ok(DataType::UInt64),
            Some(Dt::Float32(_)) => Ok(DataType::Float32),
            Some(Dt::Float64(_)) => Ok(DataType::Float64),
            Some(Dt::Decimal128(d)) => Ok(DataType::Decimal128(
                usize::try_from(d.precision).unwrap_or(usize::MAX),
                usize::try_from(d.scale).unwrap_or(usize::MAX),
            )),
            Some(Dt::Timestamp(t)) => Ok(DataType::Timestamp(
                time_unit_from_proto(t.time_unit)?,
                t.timezone,
            )),
            Some(Dt::Date(_)) => Ok(DataType::Date),
            Some(Dt::Time(unit)) => Ok(DataType::Time(time_unit_from_proto(unit)?)),
            Some(Dt::Duration(unit)) => Ok(DataType::Duration(time_unit_from_proto(unit)?)),
            Some(Dt::Interval(_)) => Ok(DataType::Interval),
            Some(Dt::Binary(_)) => Ok(DataType::Binary),
            Some(Dt::FixedSizeBinary(size)) => {
                Ok(DataType::FixedSizeBinary(usize::try_from(size).unwrap_or(usize::MAX)))
            }
            Some(Dt::Utf8(_)) => Ok(DataType::Utf8),
            Some(Dt::FixedSizeList(l)) => Ok(DataType::FixedSizeList(
                Box::new(
                    l.dtype
                        .ok_or_else(|| DaftError::ValueError("FixedSizeListType missing dtype".into()))?
                        .into_daft()?,
                ),
                usize::try_from(l.size).unwrap_or(usize::MAX),
            )),
            Some(Dt::List(dtype)) => Ok(DataType::List(Box::new(dtype.into_daft()?))),
            Some(Dt::Struct(s)) => Ok(DataType::Struct(
                s.fields.into_iter().map(proto_field_into_daft).collect::<DaftResult<_>>()?,
            )),
            Some(Dt::Map(m)) => Ok(DataType::Map {
                key: Box::new(
                    m.key
                        .ok_or_else(|| DaftError::ValueError("MapType missing key dtype".into()))?
                        .into_daft()?,
                ),
                value: Box::new(
                    m.value
                        .ok_or_else(|| DaftError::ValueError("MapType missing value dtype".into()))?
                        .into_daft()?,
                ),
            }),
            Some(Dt::Extension(e)) => Ok(DataType::Extension(
                e.name,
                Box::new(
                    e.dtype
                        .ok_or_else(|| DaftError::ValueError("ExtensionType missing dtype".into()))?
                        .into_daft()?,
                ),
                e.metadata,
            )),
            Some(Dt::Embedding(e)) => Ok(DataType::Embedding(
                Box::new(
                    e.dtype
                        .ok_or_else(|| DaftError::ValueError("EmbeddingType missing dtype".into()))?
                        .into_daft()?,
                ),
                usize::try_from(e.size).unwrap_or(usize::MAX),
            )),
            Some(Dt::Image(i)) => Ok(DataType::Image(
                i.mode.map(image_mode_from_proto).transpose()?,
            )),
            Some(Dt::FixedShapeImage(i)) => Ok(DataType::FixedShapeImage(
                image_mode_from_proto(i.mode)?,
                i.height,
                i.width,
            )),
            Some(Dt::Tensor(dtype)) => Ok(DataType::Tensor(Box::new(dtype.into_daft()?))),
            Some(Dt::FixedShapeTensor(t)) => Ok(DataType::FixedShapeTensor(
                Box::new(
                    t.dtype
                        .ok_or_else(|| DaftError::ValueError("FixedShapeTensorType missing dtype".into()))?
                        .into_daft()?,
                ),
                t.shape,
            )),
            Some(Dt::SparseTensor(t)) => Ok(DataType::SparseTensor(
                Box::new(
                    t.dtype
                        .ok_or_else(|| DaftError::ValueError("SparseTensorType missing dtype".into()))?
                        .into_daft()?,
                ),
                t.indices_offset,
            )),
            Some(Dt::FixedShapeSparseTensor(t)) => Ok(DataType::FixedShapeSparseTensor(
                Box::new(
                    t.dtype
                        .ok_or_else(|| DaftError::ValueError("missing dtype".into()))?
                        .into_daft()?,
                ),
                t.shape,
                t.indices_offset,
            )),
            #[cfg(feature = "python")]
            Some(Dt::Python(_)) => Ok(DataType::Python),
            #[cfg(not(feature = "python"))]
            Some(Dt::Python(_)) => {
                unsupported("DataType::Python on a pure-Rust (non-python) build")
            }
            Some(Dt::Unknown(_)) => Ok(DataType::Unknown),
            Some(Dt::File(media_type)) => Ok(DataType::File(media_type_from_proto(media_type)?)),
        }
    }
}

pub(crate) fn time_unit_to_proto(unit: TimeUnit) -> proto::TimeUnit {
    match unit {
        TimeUnit::Nanoseconds => proto::TimeUnit::Nanoseconds,
        TimeUnit::Microseconds => proto::TimeUnit::Microseconds,
        TimeUnit::Milliseconds => proto::TimeUnit::Milliseconds,
        TimeUnit::Seconds => proto::TimeUnit::Seconds,
    }
}

pub(crate) fn time_unit_from_proto(unit: i32) -> DaftResult<TimeUnit> {
    match proto::TimeUnit::try_from(unit) {
        Ok(proto::TimeUnit::Nanoseconds) => Ok(TimeUnit::Nanoseconds),
        Ok(proto::TimeUnit::Microseconds) => Ok(TimeUnit::Microseconds),
        Ok(proto::TimeUnit::Milliseconds) => Ok(TimeUnit::Milliseconds),
        Ok(proto::TimeUnit::Seconds) => Ok(TimeUnit::Seconds),
        _ => invalid(format!("unknown TimeUnit {unit}")),
    }
}

pub(crate) fn image_mode_to_proto(mode: ImageMode) -> proto::ImageMode {
    match mode {
        ImageMode::L => proto::ImageMode::L,
        ImageMode::LA => proto::ImageMode::La,
        ImageMode::RGB => proto::ImageMode::Rgb,
        ImageMode::RGBA => proto::ImageMode::Rgba,
        ImageMode::L16 => proto::ImageMode::L16,
        ImageMode::LA16 => proto::ImageMode::La16,
        ImageMode::RGB16 => proto::ImageMode::Rgb16,
        ImageMode::RGBA16 => proto::ImageMode::Rgba16,
        ImageMode::RGB32F => proto::ImageMode::Rgb32f,
        ImageMode::RGBA32F => proto::ImageMode::Rgba32f,
    }
}

pub(crate) fn image_mode_from_proto(mode: i32) -> DaftResult<ImageMode> {
    match proto::ImageMode::try_from(mode) {
        Ok(proto::ImageMode::L) => Ok(ImageMode::L),
        Ok(proto::ImageMode::La) => Ok(ImageMode::LA),
        Ok(proto::ImageMode::Rgb) => Ok(ImageMode::RGB),
        Ok(proto::ImageMode::Rgba) => Ok(ImageMode::RGBA),
        Ok(proto::ImageMode::L16) => Ok(ImageMode::L16),
        Ok(proto::ImageMode::La16) => Ok(ImageMode::LA16),
        Ok(proto::ImageMode::Rgb16) => Ok(ImageMode::RGB16),
        Ok(proto::ImageMode::Rgba16) => Ok(ImageMode::RGBA16),
        Ok(proto::ImageMode::Rgb32f) => Ok(ImageMode::RGB32F),
        Ok(proto::ImageMode::Rgba32f) => Ok(ImageMode::RGBA32F),
        _ => invalid(format!("unknown ImageMode {mode}")),
    }
}

pub(crate) fn media_type_to_proto(media: MediaType) -> proto::MediaType {
    match media {
        MediaType::Unknown => proto::MediaType::Unknown,
        MediaType::Video => proto::MediaType::Video,
        MediaType::Audio => proto::MediaType::Audio,
    }
}

pub(crate) fn media_type_from_proto(media: i32) -> DaftResult<MediaType> {
    match proto::MediaType::try_from(media) {
        Ok(proto::MediaType::Unknown) => Ok(MediaType::Unknown),
        Ok(proto::MediaType::Video) => Ok(MediaType::Video),
        Ok(proto::MediaType::Audio) => Ok(MediaType::Audio),
        _ => invalid(format!("unknown MediaType {media}")),
    }
}

// ---------------------------------------------------------------------------
// Field / Schema
// ---------------------------------------------------------------------------

pub fn field_to_proto(field: &Field) -> DaftResult<proto::Field> {
    Ok(proto::Field {
        name: field.name.to_string(),
        dtype: Some(field.dtype.to_proto()?),
        metadata: field.metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    })
}

pub(crate) fn proto_field_into_daft(field: proto::Field) -> DaftResult<Field> {
    let dtype = field
        .dtype
        .ok_or_else(|| DaftError::ValueError("Field missing dtype".into()))?
        .into_daft()?;
    let mut metadata = Metadata::new();
    for (k, v) in field.metadata {
        metadata.insert(k, v);
    }
    Ok(Field::new(field.name, dtype).with_metadata(Arc::new(metadata)))
}

pub trait SchemaProto {
    fn to_proto(&self) -> DaftResult<proto::Schema>;
}

pub trait ProtoSchema {
    fn into_daft(self) -> DaftResult<SchemaRef>;
}

impl SchemaProto for Schema {
    fn to_proto(&self) -> DaftResult<proto::Schema> {
        Ok(proto::Schema {
            fields: self.fields().iter().map(field_to_proto).collect::<DaftResult<_>>()?,
        })
    }
}

impl ProtoSchema for proto::Schema {
    fn into_daft(self) -> DaftResult<SchemaRef> {
        let fields = self
            .fields
            .into_iter()
            .map(proto_field_into_daft)
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }
}

// ---------------------------------------------------------------------------
// Series IPC helpers (used by Literal and RangeRepartition boundaries)
// ---------------------------------------------------------------------------

