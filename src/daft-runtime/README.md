# daft-runtime

单机 Daft 执行服务端（非分布式）：**纯 Rust HTTP 服务，二进制不嵌入
CPython**。它接收客户端序列化好的逻辑计划（protobuf `LogicalPlan`，扫描节点
已物化为 `ScanTask`），用 Daft 原生执行引擎运行，并把结果以 Arrow IPC 信封
返回给 Python 客户端。

**线上协议一律使用 protobuf，不使用 JSON。** 协议 schema 位于
`src/daft-protocol/proto/daft/v1/`（`runtime.proto` 定义 HTTP 控制面，
`worker.proto` 定义 runtime 与 Python worker 子进程之间的管道消息），同一份
schema 由 `build.rs`（prost）编译为 Rust 类型，并生成 Python 绑定
`daft/runtime/daft_proto/`，保证客户端与 Python worker 子进程和 Rust 服务端
逐字节说同一种协议。

## 架构

```text
                    POST /v1/jobs  application/x-protobuf
                    (JobSubmitRequest{ requires_udf, logical_plan, partition_sets })
Python 客户端  ──────────────────────────────────────────────►  daft-runtime (Rust)
                                                                     │
                                       ┌─────────────────────────────┼───────────────────────────┐
                                       │ requires_udf = false        │ requires_udf = true        │
                                       ▼                             ▼                             │
                                native::execute_plan_native   python -m daft.runtime.worker      │
                                （protobuf 解码 → 优化 →       （短生命周期子进程，仅 UDF 计划）    │
                                 translate → 本地执行引擎）            │                           │
                                       │                             │ stdin/stdout:             │
                                       │                             │ u32 LE 长度前缀 +          │
                                       │                             │ protobuf WorkerRequest/   │
                                       │                             │ WorkerResponse            │
                                       └───────────────┬─────────────┘                           │
                                                       ▼                                          │
                                           DAFTRES1 IPC 结果信封 ◄─────────────────────────────────┘
```

执行路由由提交方在序列化时计算的 `requires_udf` 标志决定：

- `requires_udf = false`：计划不包含 Python UDF，纯 Rust 路径负责解码、
  优化、下推为本地物理计划并执行。服务端可以在没有任何 Python 解释器的
  环境下运行。
- `requires_udf = true`：计划包含 Python UDF（cloudpickle 字节无法在 Rust
  侧解码），服务端临时 spawn 一个 `python -m daft.runtime.worker` 子进程，
  向 stdin 写入一个长度前缀（4 字节小端）的 `WorkerRequest` protobuf，从
  stdout 读回同样帧格式的 `WorkerResponse` protobuf（成功时携带 `DAFTRES1`
  结果信封，失败时携带错误信息）。子进程执行完即退出，解释器不会常驻服务端
  进程。

因此“运行时携带 Python”只发生在真正需要执行 Python UDF 的那一刻，且 Python
解释器属于独立的 worker 子进程，而不是嵌入 runtime 二进制。

## 构建

```bash
cargo build -p daft-runtime
```

二进制不链接 Python，构建环境也不需要安装/编译 CPython。

## 运行

```bash
# 默认监听 127.0.0.1:8080
./target/debug/daft-runtime

# 自定义地址与鉴权 token（客户端需带 Authorization: Bearer <token>）
DAFT_RUNTIME_ADDRESS=0.0.0.0:9000 DAFT_RUNTIME_TOKEN=secret ./target/debug/daft-runtime
```

### UDF worker 的 Python 解释器选择

只有 `requires_udf = true` 的任务才会需要 Python，解释器按以下顺序解析：

1. `DAFT_RUNTIME_PYTHON` 指向的解释器（绝对路径，或相对于仓库根目录的
   相对路径，例如 `.venv/bin/python`）；
2. 仓库根目录下的 `.venv/bin/python`；
3. `PATH` 中的 `python3` / `python`。

worker 模块位于 Python 包 `daft/runtime/worker.py`，即仓库的 `daft`
目录必须能被该解释器导入（worker 会按需把仓库根目录加入 `sys.path`）。

## HTTP API（v3）

所有控制面请求/响应的 `Content-Type` 都是 `application/x-protobuf`；错误响应
body 是 `daft.v1.Error` protobuf；成功结果由 `daft.v1.JobResult` 包裹，其
`payload` 是 `DAFTRES1` 的 Arrow IPC 结果信封。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/jobs` | 提交任务，body 为 `JobSubmitRequest`（见下），返回 `JobSubmitResponse` |
| GET | `/v1/jobs/{id}` | 查询任务状态（pending/running/succeeded/failed/canceled） |
| GET | `/v1/jobs/{id}/result` | 成功时返回 protobuf `JobResult`，`payload` 为 `DAFTRES1` 信封 |
| DELETE | `/v1/jobs/{id}` | 取消 pending 任务 |
| GET | `/v1/workers` | 已注册 worker（预留） |
| POST | `/v1/udf-artifacts` | 上传 UDF 产物（内容寻址，sha256） |
| GET | `/v1/udf-artifacts/{id}` / `.../payload` | 查询/下载 UDF 产物 |

`JobSubmitRequest` 字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `requires_udf` | bool | 计划是否包含 Python UDF，决定 native / worker 路由 |
| `logical_plan` | bytes | 不透明的 protobuf 计划字节（`LogicalPlanBuilder.to_bytes()` 产物，含 UDF 时已内嵌 cloudpickle 字节；runtime 只负责搬运，不重建计划 AST） |
| `partition_sets` | map<string, bytes> | 内存分区集合：cache key → 原始 Arrow IPC blob（`u32 LE` 数量 + 每分区 `u64 LE` 长度 + IPC 流，不再 base64） |
| `udf_artifact_ids` | repeated string | 任务依赖的 UDF 产物 id（先 `POST /v1/udf-artifacts` 上传） |

## UDF 产物

`POST /v1/udf-artifacts` 上传 `UploadUdfArtifactRequest{ metadata, payload }`，
服务端按 payload 计算 SHA-256 并返回内容寻址的 `sha256:<hex>` artifact id。
提交任务时通过 `udf_artifact_ids` 引用。服务端在任务执行前把产物物化到每任务
临时目录，并把该目录加入 worker 的 `extra_paths`（worker 会把它前置到
`sys.path`），因此产物必须是一个**按文件名可导入**的 Python 模块（`.py`）、
zip 或 wheel。`UdfArtifactMetadata.filename` 指定物化后的文件名（默认为空时
服务端从 `entrypoint` 推导 `<module>.py`）；文件名会经过净化，杜绝路径穿越。
提交时先校验 artifact 存在（快速失败），任务结束后清理临时目录。

完整链路（客户端 -> Rust 服务端 -> Python worker）：

```python
import daft
from daft import col
from daft.runtime.client import RuntimeClient
from daft.plan_transport import serialize_plan_parts

client = RuntimeClient("http://127.0.0.1:8080", token="secret")

# 1. 上传一个可导入的模块 artifact
artifact_id = client.upload_udf_artifact(
    b"def helper(x):\n    return x * 100\n",
    entrypoint="helper_mod:helper",
    filename="helper_mod.py",
)

# 2. UDF 内部 import 该模块（客户端进程里没有它，只有 worker 能 import）
@daft.func
def via_artifact(x: int) -> int:
    import helper_mod
    return helper_mod.helper(x)

df = daft.from_pydict({"x": [1, 2, 3]}).with_column("y", via_artifact(col("x")))

# 3. 序列化 + 提交，引用 artifact id
plan, requires_udf, partition_sets = serialize_plan_parts(df)
job = client.submit(
    plan,
    requires_udf=requires_udf,
    partition_sets=partition_sets,
    udf_artifact_ids=[artifact_id],
)
print(job.result()[0].to_pydict())  # {"x": [1, 2, 3], "y": [100, 200, 300]}
```

引用不存在的 artifact 会在提交时被同步拒绝（400，protobuf `Error` body）。

## 协议 schema 与 Python 绑定

- schema：`src/daft-protocol/proto/daft/v1/{runtime,worker}.proto`
- Rust：`build.rs` 用 prost 编译，类型在 `daft_protocol::daft::v1`
- Python：`daft/runtime/daft_proto/` 下由 protoc 生成的 `*_pb2.py`

Python 绑定必须用与运行时 protobuf 库兼容的 protoc 生成（仓库 venv 使用
protobuf 5.x，对应 protoc 29.x；protoc 33+ 生成的 gencode 要求 protobuf
≥6.33 会导入失败）。用仓库自带脚本重新生成，脚本会校验 protoc 与运行时版本
兼容性并做导入/往返冒烟测试：

```bash
.venv/bin/python tools/gen_daft_proto.py
# 或显式指定 protoc：
PROTOC=/path/to/protoc .venv/bin/python tools/gen_daft_proto.py

# CI 用 --check 验证已入库绑定与 schema 无漂移（漂移时非零退出）
.venv/bin/python tools/gen_daft_proto.py --check
```

## Python 客户端

```python
import daft

daft.connect("http://127.0.0.1:8080", token="secret")

df = (
    daft.read_json("data/*.json")
    .with_column("double", daft.col("id") * 2)
    .filter(daft.col("id") > 1)
)
print(df.collect().to_pydict())
```

低层 API 见 `daft/runtime/client.py`（`RuntimeClient.submit/status/result`，
以及 `upload_udf_artifact`），序列化工具见 `daft/plan_transport.py`
（`serialize_plan_parts` 返回 `(plan_bytes, requires_udf, partition_sets)`，
partition_sets 为原始字节）。Python 侧只负责构建并发送计划，服务端始终是
Rust 二进制；UDF 检测在序列化时于 Python 侧完成（`requires_udf` 标志随计划
一起发送），Rust 侧只按标志路由，不再扫描计划。

## 测试

端到端测试在 `tests/runtime/test_remote_runtime.py`，覆盖：

- Rust 服务端在**没有 Python** 的环境下执行无 UDF 计划；
- 含 UDF 的计划正确经过 Python worker 子进程执行（含 UDF + 聚合）；
- `requires_udf` 标志的路由行为（纯 Rust 路径可用、UDF 路径在解释器缺失时
  明确报错）。
- UDF artifact 的上传/查询/下载端点（Rust 二进制）；
- UDF artifact 的完整链路：上传 -> 计划引用 -> worker 内 import 执行
  （Rust 二进制），含未知 artifact 的提交时快速失败。
- 高层用户路径：`daft.connect()` -> `DataFrame.collect()` 对 Rust 二进制的
  native 与 UDF 两条路线端到端执行。

```bash
DAFT_RUNNER=native .venv/bin/python -m pytest tests/runtime/test_remote_runtime.py -v
```
