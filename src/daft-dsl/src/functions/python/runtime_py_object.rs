#![allow(clippy::all, reason = "todo: remove; getting a rustc error")]

use std::{
    fmt,
    sync::Arc,
};

#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, Visitor},
    ser::Error as SerError,
};

/// A wrapper around a Python object that is serde-able even when the Python
/// feature flag is turned off.
///
/// The serde representation is always a byte array holding the pickled
/// object (``daft.pickle``/cloudpickle). Builds with the ``python`` feature
/// pickle the live object on serialize and unpickle on deserialize; builds
/// without it simply round-trip the opaque bytes produced by a Python-enabled
/// build. This is what lets a plan containing Python UDFs travel through
/// pure-Rust processes (the scheduler and the executor never link CPython)
/// without losing the embedded closure payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuntimePyObject {
    #[cfg(feature = "python")]
    obj: PyObjectWrapper,
    #[cfg(not(feature = "python"))]
    raw_bytes: Vec<u8>,
}

impl RuntimePyObject {
    /// Creates a new 'None' python value as 'None' is used where a non-optional RuntimePyObject is expected.
    pub fn new_none() -> Self {
        use std::sync::Arc;

        #[cfg(feature = "python")]
        {
            let none_value = Arc::new(pyo3::Python::attach(|py| py.None()));
            Self {
                obj: PyObjectWrapper(none_value),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            // cloudpickle (protocol 5) of Python's ``None``: ``\x80\x05N.``
            // Kept valid so a value created in a non-Python build and later
            // serialized can still be unpickled by the Python UDF worker.
            Self {
                raw_bytes: b"\x80\x05N.".to_vec(),
            }
        }
    }

    #[cfg(feature = "python")]
    pub fn new(value: Arc<pyo3::Py<pyo3::PyAny>>) -> Self {
        Self {
            obj: PyObjectWrapper(value),
        }
    }

    #[cfg(feature = "python")]
    pub fn unwrap(self) -> Arc<pyo3::Py<pyo3::PyAny>> {
        self.obj.0
    }
}

impl Serialize for RuntimePyObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[cfg(feature = "python")]
        {
            let bytes = pyo3::Python::attach(|py| {
                common_py_serde::pickle_dumps(py, &self.obj.0)
                    .map_err(|e| SerError::custom(e.to_string()))
            })?;
            serializer.serialize_bytes(&bytes)
        }
        #[cfg(not(feature = "python"))]
        {
            serializer.serialize_bytes(&self.raw_bytes)
        }
    }
}

struct RuntimePyObjectVisitor;

impl<'de> Visitor<'de> for RuntimePyObjectVisitor {
    type Value = RuntimePyObject;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array containing a pickled Python object")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        #[cfg(feature = "python")]
        {
            let obj = pyo3::Python::attach(|py| {
                common_py_serde::pickle_loads(py, v)
                    .map(|bound| bound.unbind())
                    .map_err(|e| DeError::custom(e.to_string()))
            })?;
            Ok(RuntimePyObject {
                obj: PyObjectWrapper(Arc::new(obj)),
            })
        }
        #[cfg(not(feature = "python"))]
        {
            Ok(RuntimePyObject {
                raw_bytes: v.to_vec(),
            })
        }
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_bytes(&v)
    }
}

impl<'de> Deserialize<'de> for RuntimePyObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(RuntimePyObjectVisitor)
    }
}

#[cfg(feature = "python")]
impl AsRef<pyo3::Py<pyo3::PyAny>> for RuntimePyObject {
    /// Retrieves a reference to the underlying pyo3::PyObject object
    fn as_ref(&self) -> &pyo3::Py<pyo3::PyAny> {
        &self.obj.0
    }
}

#[cfg(feature = "python")]
impl From<pyo3::Py<pyo3::PyAny>> for RuntimePyObject {
    fn from(value: pyo3::Py<pyo3::PyAny>) -> Self {
        Self::new(Arc::new(value))
    }
}
