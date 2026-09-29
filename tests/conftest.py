"""pytest 全局配置。

默认把 orchestrator 的 INFO 日志接到根 logger，保证单测运行时能直接看到：
- [调度输入]：任务需求、选择器、候选节点的资源/标签/版本；
- [判定依据]：每个候选节点被接受或拒绝的具体原因；
- [最终归属]：任务落到哪个节点，或因何原因被拒。

使用 ``uv run pytest -s`` 可在终端直接观察；默认也会被 caplog 捕获以便断言。
"""

from __future__ import annotations

import logging

import pytest

_ORCH_LOGGER = logging.getLogger("orchestrator")


@pytest.fixture(autouse=True)
def _emit_orchestrator_logs(caplog):
    caplog.set_level(logging.INFO, logger="orchestrator")
    yield


def pytest_configure(config: pytest.Config) -> None:
    if not _ORCH_LOGGER.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        _ORCH_LOGGER.addHandler(handler)
    _ORCH_LOGGER.setLevel(logging.INFO)
    _ORCH_LOGGER.propagate = True
