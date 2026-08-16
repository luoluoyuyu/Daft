from __future__ import annotations

import copy
import warnings

import pytest

# This module tests legacy @daft.udf features (override_options, with_concurrency) with no new-API equivalent.
warnings.filterwarnings("ignore", category=DeprecationWarning, message=r".*@daft\.udf.*")
pytestmark = pytest.mark.filterwarnings(r"ignore:.*@daft\.udf.*:DeprecationWarning")

import daft
from daft import udf
from daft.daft import SystemInfo
from daft.expressions import col


DATA = {"id": [i for i in range(100)]}


@udf(return_dtype=daft.DataType.int64())
def my_udf(c):
    return [1] * len(c)


###
# Test behavior of overriding options
###


def test_partial_resource_request_overrides():
    new_udf = my_udf.override_options(num_cpus=1.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes is None

    new_udf = new_udf.override_options(num_gpus=8.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus == 8.0
    assert new_udf.resource_request.memory_bytes is None

    new_udf = new_udf.override_options(num_gpus=None)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes is None

    new_udf = new_udf.override_options(memory_bytes=100)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes == 100


def test_resource_request_pickle_roundtrip():
    new_udf = my_udf.override_options(num_cpus=1.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes is None

    assert new_udf == copy.deepcopy(new_udf)

    new_udf = new_udf.override_options(num_gpus=8.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus == 8.0
    assert new_udf.resource_request.memory_bytes is None
    assert new_udf == copy.deepcopy(new_udf)


def test_requesting_too_many_cpus():
    df = daft.from_pydict(DATA)

    my_udf_parametrized = my_udf.override_options(num_cpus=1000)
    df = df.with_column(
        "foo",
        my_udf_parametrized(col("id")),
    )

    with pytest.raises(Exception):
        df.collect()


def test_requesting_too_much_memory():
    df = daft.from_pydict(DATA)
    system_info = SystemInfo()

    my_udf_parametrized = my_udf.override_options(memory_bytes=system_info.total_memory() + 1)
    df = df.with_column(
        "foo",
        my_udf_parametrized(col("id")),
    )

    with pytest.raises(Exception):
        df.collect()


###
# GPU tests - can only run if machine has a GPU
###


def test_improper_num_gpus():
    with pytest.raises(ValueError, match="DaftError::ValueError"):

        @udf(return_dtype=daft.DataType.int64(), num_gpus=-1)
        def foo(c):
            return c

    with pytest.raises(ValueError, match="DaftError::ValueError"):

        @udf(return_dtype=daft.DataType.int64(), num_gpus=1.5)
        def foo(c):
            return c

    @udf(return_dtype=daft.DataType.int64())
    def foo(c):
        return c

    with pytest.raises(ValueError, match="DaftError::ValueError"):
        foo = foo.override_options(num_gpus=-1)

    with pytest.raises(ValueError, match="DaftError::ValueError"):
        foo = foo.override_options(num_gpus=1.5)
