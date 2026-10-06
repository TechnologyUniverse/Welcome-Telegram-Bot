"""Container liveness check based on the event-loop heartbeat in /tmp."""
import os
from pathlib import Path
import stat
import sys
import time

HEALTH_FILE = Path("/tmp/welcome_bot_health")
MAX_AGE_SECONDS = 75


def main() -> int:
    fd = None
    try:
        fd = os.open(HEALTH_FILE, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            return 1
        with os.fdopen(fd, "r", encoding="ascii") as health_file:
            fd = None
            raw_pid = health_file.read(32).strip()
            if health_file.read(1):
                return 1
        pid = int(raw_pid)
        if pid <= 0:
            return 1
        age = time.time() - info.st_mtime
        os.kill(pid, 0)
        if age < 0 or age > MAX_AGE_SECONDS:
            return 1
    except (OSError, ValueError):
        return 1
    finally:
        if fd is not None:
            os.close(fd)
    return 0


if __name__ == "__main__":
    sys.exit(main())
