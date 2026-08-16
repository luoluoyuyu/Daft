# UniteStream 作业脚本示例 · 有状态类 UDF（VLA 前向演示）
#
# 用法：
#   1. Copilot / DataFrame 作业：只粘贴下方「SCRIPT BODY」段（不要 import、不要 if __name__）
#   2. 本地验证（需已安装定制 daft + unite-stream）：
#        cd ../Daft && .venv/bin/python -c "
#        from unite_stream import UniteStreamClient
#        body = open('unite-stream/examples/vla_forward_job.py').read().split('SCRIPT BODY')[1]
#        print(UniteStreamClient(system_target_dir='/tmp/vla_demo').submit(body)[0])
#        "
#
# 正式环境请把 from_pydict 换成 UniteStream.read_parquet(\"<数据集 read_path>\")

# ----- SCRIPT BODY（从此行起粘贴到作业编辑器） -----

@cls(max_concurrency=1)
class VlaForwardEngine:
    """Worker 进程内只构造一次；模拟大模型权重加载。"""

    def __init__(self):
        self._loss_scale = 0.001

    @method(
        return_dtype=UniteStream.DataType.struct(
            {
                "computed_loss": UniteStream.DataType.float64(),
                "text_prompt": UniteStream.DataType.string(),
            }
        ),
    )
    def process_and_forward(self, frame_index: int, raw_action: float, instruction: str):
        clipped = max(-1.0, min(1.0, float(raw_action)))
        token = int((clipped + 1.0) / 2.0 * 255) + 32000
        loss = float(frame_index) * 0.01 + token * self._loss_scale
        return {"computed_loss": loss, "text_prompt": str(instruction)}


# 演示数据；上线后改为：
# df = UniteStream.read_parquet("s3://<bucket>/<org>/datasets/.../**/*.parquet")
df = UniteStream.from_pydict(
    {
        "camera_video_path": ["s3://demo/ep0/cam.mp4"] * 4,
        "frame_index": [0, 1, 2, 3],
        "action": [-0.5, 0.0, 0.5, 1.0],
    }
)

df = df.with_column("instruction", UniteStream.lit("pick up the red apple"))

engine = VlaForwardEngine()
df = df.with_column(
    "vla_metrics",
    engine.process_and_forward(
        df["frame_index"],
        df["action"],
        df["instruction"],
    ),
)

OUTPUT_STREAMS.append(
    df.select("camera_video_path", "frame_index", "action", "instruction", "vla_metrics")
)

# ----- END SCRIPT BODY -----
