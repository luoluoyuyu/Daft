# unite-stream

独立 Python 包，在 [Daft](https://github.com/Eventual-Inc/Daft) 之上实现 **Parser / Runtime 分离**。

- **与 `daft` 完全独立**：`import unite_stream` 与 `import daft` 是两个包；调用方代码不需要 `import daft`。
- **零 `import` 用户脚本**：Compiler 自动注入 `UniteStream` / `func` / `cls` / `method` / `udf` / `metrics` / `daft`，用户脚本无需任何 `import` 即可直接使用。
- **完全信任的脚本执行环境**：用户脚本以完整 Python builtins 运行，可自由 `import` 标准库 / 用户库、定义类与装饰器、使用 `eval` / `exec` 等动态求值原语；不做静态校验。
- **线程安全 Parser**：`UniteStreamCompiler` 构造后不可变，`compile_to_ir` 内部使用全新 globals + 独立 `job_namespace` 输出目录，可被多线程共享。
- **仅 DataFrame**：**不支持** SQL（`sql` / `sql_expr` / `read_sql` 不暴露）。
- **UDF 完整支持**：`@func` / `@cls` + `@method.batch` / 旧版 `@udf`，行为与 Daft 一致。

## 安装

> **重要**：unite-stream 依赖**本仓库内定制的 Daft**（新增了 `CompiledLogicalPlan`、
> `from_compiled_logical_plan`、`plan_transport` 等 Parser 阶段 API），**不能**直接用 PyPI 上的 daft。
> 为此 Daft 用 PEP 440 local segment 打成 `daft==0.3.0.dev0+unite`，并与 unite-stream
> 作为一个 **bundle** 一起分发。

### 方式 A：monorepo 开发（editable，最快）

```bash
cd /path/to/Daft
make .venv && make build              # 编译并 editable 安装定制 Daft
.venv/bin/pip install -e unite-stream # editable 装 unite-stream
```

### 方式 B：双 wheel bundle（推荐用于发布给别的项目）

在 Daft monorepo 内一键产出 bundle：

```bash
cd /path/to/Daft
make bundle
```

产物：

```
dist/unite-stream-bundle/
├── wheels/
│   ├── daft-0.3.0.dev0+unite-*.whl    ← 定制 Daft（含 Rust 二进制）
│   └── unite_stream-0.1.0-py3-none-any.whl
├── requirements.txt
├── install.sh                         ← 一键安装脚本
└── README.txt
dist/unite-stream-bundle.tar.gz        ← 同上内容的 tarball
```

在**任意别的项目**里使用 bundle（无需 monorepo、无需 Rust 工具链）：

```bash
# 拷过去
tar xzf unite-stream-bundle.tar.gz
cd unite-stream-bundle

# 方案 1：一键脚本
./install.sh

# 方案 2：手动
pip install -r requirements.txt
```

`requirements.txt` 长这样（严格 pin 到 local segment，永远不会被 PyPI 上的 daft 覆盖）：

```
--find-links ./wheels
daft==0.3.0.dev0+unite
unite-stream==0.1.0
```

### 方式 C：从本仓库安装（需已安装定制 Daft）

```bash
# 先安装定制 Daft（见 [luoluoyuyu/Daft](https://github.com/luoluoyuyu/Daft) 的 make bundle 或 make build）
pip install "git+ssh://git@github.com/luoluoyuyu/unite_stream.git@main"
# 或 editable：git clone git@github.com:luoluoyuyu/unite_stream.git && pip install -e unite_stream
```

## 快速上手（4 种入口，按需选用）

### A. 一站式 façade（推荐）

```python
from unite_stream import UniteStreamClient

script = """
df = UniteStream.from_pydict({"id": [1, 2, 3], "age": [18, 25, 30]})
OUTPUT_STREAMS.append(df.filter(UniteStream.col("age") >= 21))
"""

client = UniteStreamClient(system_target_dir="./lake/out/")
results = client.submit(script)             # → list[daft.DataFrame]
print(results[0])
```

### B. 模块级快捷函数

```python
from unite_stream import parse, submit

# 仅解析，拿到未绑定的 Daft plan：
plans = parse(script)                       # → list[daft.DataFrame]

# 端到端：解析 + 编译 + 执行：
results = submit(script, system_target_dir="./lake/out/")
```

### C. 分阶段（Parser → Compiler → Runtime）

```python
from unite_stream import UniteStreamParser, UniteStreamCompiler, UniteStreamRuntime

# 1. Parser：返回原生 Daft 的 lazy plans，外部随便处理
parser = UniteStreamParser()
plans = parser.parse(script)                # → list[daft.DataFrame]

# 2. Compiler：parse + 绑定 write_parquet + cloudpickle 序列化
compiler = UniteStreamCompiler(system_target_dir="./lake/out/")
ir_bytes = compiler.compile_and_extract_ir(script)

# 3. Runtime：反序列化执行（或直接 execute_plans）
runtime = UniteStreamRuntime(distributed_mode=False)
results = runtime.execute_ir(ir_bytes)
# results = runtime.execute_plans(plans)    # ← 也行，绕过 IR
```

### D. 只要 plans，不写盘

```python
from unite_stream import UniteStreamClient
client = UniteStreamClient(system_target_dir="./scratch/")
plans = client.parse(script)
results = client.execute_plans(plans)       # 不经过 write_parquet 绑定
```

> 注意：**调用方代码完全不需要 `import daft`**，用户脚本里也不需要 `import unite_stream`。

### UDF + 用户 `import`

```python
script = """
import math

@func
def norm(x):
    return math.sqrt(float(x))

df = UniteStream.from_pydict({"v": [3.0, 4.0]})
OUTPUT_STREAMS.append(df.with_column("n", norm(UniteStream.col("v"))))
"""
```

有状态 / 批 UDF：

```python
script = """
@cls
class Badge:
    def __init__(self):
        self.suffix = "-vip"

    @method.batch(return_dtype=UniteStream.DataType.string())
    def label(self, name):
        return [n + self.suffix for n in name.to_pylist()]

users = UniteStream.from_pydict({"name": ["amy", "ben"]})
OUTPUT_STREAMS.append(users.with_column("tag", Badge().label(UniteStream.col("name"))))
"""
```

### 多线程并发（互不影响）

```python
from concurrent.futures import ThreadPoolExecutor
from unite_stream import UniteStreamCompiler, UniteStreamRuntime

compiler = UniteStreamCompiler(system_target_dir="./lake/out/")
runtime = UniteStreamRuntime()

def run(name: str, script: str) -> int:
    ir = compiler.compile_and_extract_ir(script, job_namespace=name)
    return len(runtime.execute_ir(ir)[0])

with ThreadPoolExecutor(max_workers=8) as pool:
    futures = [pool.submit(run, f"job_{i}", SCRIPT) for i in range(8)]
    for fut in futures:
        print(fut.result())
```

每个线程：

- 拿到 **独立** 的 globals dict / `OUTPUT_STREAMS` 列表；
- 写入 **独立** 的目录 `{target_dir}/{job_namespace}/stream_job_<i>.parquet`；
- `job_namespace` 缺省为 UUID hex，自动避免冲突。

## API 概览

`UniteStream` 命名空间 **逐项显式映射** `daft` 顶层公开 API（不含 SQL）：

| 类别 | 数量 | 示例 |
| --- | --- | --- |
| `read_*` / `from_*` / `range` / `read_table` | 24 | `UniteStream.read_parquet(...)` |
| 表达式 helper | 4 | `UniteStream.col`, `lit`, `element`, `interval` |
| UDF 装饰器 | 5 | `UniteStream.func`, `cls`, `method`, `udf`, `metrics` |
| context / session / catalog 透传 | 50 | `UniteStream.session()`, `UniteStream.set_execution_config(...)` |
| 类 | 20 | `UniteStream.DataType`, `Expression`, `Schema`, `Window`, `Session` ... |
| 子模块 | 1 | `UniteStream.functions` |

`UniteStreamDataFrame` 提供 **52 个变换方法** + 35 个禁止 stub + `groupby` + 元数据；`UniteStreamGroupedDataFrame` 提供 **14 个聚合方法**。

**保持不暴露**：

- **SQL 接口**：`sql` / `sql_expr` / `read_sql`
- DataFrame 物化 / 导出：`collect` / `show` / `count_rows` / `explain` / 全部 `write_*` / 全部 `to_*` / `iter_rows` / `iter_partitions` / `metrics` / `num_partitions` / `pivot` / `from_plan_bytes` / `to_plan_bytes`
- 顶层 Parser/Runtime 边界：`serialize_plan` / `deserialize_plan` / `execute_plan` / `execute_from_bytes` / `set_runner_native` / `set_runner_ray` / `get_or_create_runner` / `get_or_infer_runner_type` / `write_table`

## 异常体系

```
UniteStreamError
├── CompilationError       脚本语法错误 / 未提交 OUTPUT_STREAMS / 提交了非法对象 / exec 抛错 / IR 序列化失败
└── RuntimeExecutionError  IR 反序列化失败 / plan.collect 抛错
```

## 测试

```bash
cd unite-stream
DAFT_RUNNER=native pytest tests/ -v
```

主要测试用例：

- 用户脚本可自由使用 `eval` / `exec` / `import`
- 编译失败语义（语法错误 / 无输出 / 非法对象类型 / 用户代码抛异常）
- 用户脚本零 `import` 自动注入
- 多线程并发 compile 隔离（`job_namespace`）
- 默认 namespace 唯一性
- 类 / 子模块 / SQL 禁用 / Plan 边界禁用断言

## 包结构

| 模块 | 说明 |
| --- | --- |
| `unite_stream.dataframe` | `UniteStreamDataFrame` — 显式 DataFrame 方法镜像（自动生成） |
| `unite_stream.grouped` | `UniteStreamGroupedDataFrame` — 显式聚合方法镜像（自动生成） |
| `unite_stream.module` | `UniteStream` — 显式顶层 `daft` 镜像（自动生成，无 SQL） |
| `unite_stream.udf` | `func` / `cls` / `method` / `udf` / `metrics` 转发 |
| `unite_stream.parser` | `UniteStreamParser` — 解析用户脚本，返回 `list[daft.DataFrame]` |
| `unite_stream.compiler` | `UniteStreamCompiler` — Parser + 绑定 `write_parquet` + cloudpickle |
| `unite_stream.runtime` | `UniteStreamRuntime` / `RunnerType` — 反序列化 + 执行 |
| `unite_stream.client` | `UniteStreamClient` — 一站式 façade |
| `unite_stream.errors` | `UniteStreamError` 等异常基类 |
| `unite_stream.wrap_util` | 类型 wrap / unwrap 工具 |
| `scripts/regenerate_api_mirror.py` | Daft 升级后重新生成镜像 |

## 设计原则

1. **显式 > 隐式**：API 镜像逐项手写（脚本生成），不用 `__getattr__`，调用关系清晰可静态分析。
2. **可重建**：所有自动生成文件都来自一个脚本，可被 Daft 升级时一键 regenerate。
3. **沙箱开放**：Parser 仅做最小必要拦截（动态求值原语），保留 Python 表达力；UDF 体本身视为可信代码。
4. **线程隔离**：Parser 全局无状态、每次调用独立 globals + 独立输出目录，可放进任何 worker pool。
