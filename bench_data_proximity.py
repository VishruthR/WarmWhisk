#!/usr/bin/env python3
"""
Sustained load for the data-proximity layer: bench_float and bench_dp in parallel.

- bench_float: --param n 1000 at 3 RPS
- bench_dp: same shape as seed_images.sh; random picsum id 1–120; 3 RPS

Invokes are non-blocking (no -r) so OpenWhisk accepts work at the target rate.
Mirror wsk usage: `wsk -i` like seed_images.sh.
"""

from __future__ import annotations

import random
import subprocess
import threading
import time

DURATION_SEC = 10 * 60
RPS = 3.0
INTERVAL = 1.0 / RPS

WSK = ["wsk", "-i", "action", "invoke"]


def _invoke(args: list[str]) -> None:
    subprocess.run(
        args,
        stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
        check=False,
    )


def worker_bench_float(deadline: float) -> None:
    cmd_base = WSK + ["bench_float", "--param", "n", "1000"]
    while time.monotonic() < deadline:
        _invoke(cmd_base)
        time.sleep(INTERVAL)


def worker_bench_dp(deadline: float) -> None:
    while time.monotonic() < deadline:
        n = random.randint(1, 120)
        filename = f"image_{n}.jpg"
        url = f"https://picsum.photos/id/{n}/300"
        cmd = WSK + [
            "bench_dp",
            "--param",
            "url",
            url,
            "--param",
            "hash",
            "deadbeef",
            "--param",
            "filename",
            filename,
            "--param",
            "max_iter",
            "100",
            "--param",
            "data_dependency",
            filename,
        ]
        _invoke(cmd)
        time.sleep(INTERVAL)


def main() -> None:
    deadline = time.monotonic() + DURATION_SEC
    t_float = threading.Thread(target=worker_bench_float, args=(deadline,), daemon=True)
    t_dp = threading.Thread(target=worker_bench_dp, args=(deadline,), daemon=True)
    t_float.start()
    t_dp.start()
    t_float.join()
    t_dp.join()


if __name__ == "__main__":
    main()
