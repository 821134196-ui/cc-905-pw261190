"""Pointwise —— 带分发、阻塞算子、有界通道与检查点恢复的本地执行器。

模块组成:
  cancellation  协作式取消令牌(可唤醒阻塞等待)
  channels      有界通道(Condition 实现, 关闭/取消会唤醒所有阻塞收发)
  checkpoint    原子检查点 + fsync 的幂等输出台账
  engine        拓扑装配、工作线程、背压/瓶颈分析、恢复与核对
  server        零依赖 HTTP 服务(REST + 静态页面)
"""

from .cancellation import CancelToken
from .channels import BoundedChannel, ChannelClosed, ChannelTimeout, EndOfStream

__all__ = [
    "CancelToken",
    "BoundedChannel",
    "ChannelClosed",
    "ChannelTimeout",
    "EndOfStream",
]

__version__ = "1.0.0"
