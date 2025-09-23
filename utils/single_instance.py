import os
import atexit
import signal


def _is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_pid_lock(pid_file: str = "/tmp/perpsbot.pid") -> None:
    """
    Ensures single instance using a PID file.
    - If PID file exists and process is alive -> raises RuntimeError
    - Else writes current PID to the file and registers cleanup
    """
    try:
        if os.path.exists(pid_file):
            try:
                with open(pid_file, "r") as f:
                    content = (f.read() or "").strip()
                    old_pid = int(content) if content.isdigit() else None
            except Exception:
                old_pid = None
            if old_pid and _is_process_alive(old_pid):
                raise RuntimeError(f"Another instance is running (pid={old_pid}).")
            # stale file
            try:
                os.remove(pid_file)
            except Exception:
                pass
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))
    except Exception as e:
        raise RuntimeError(f"Failed to acquire PID lock: {e}")

    def _cleanup(*_args):
        try:
            if os.path.exists(pid_file):
                with open(pid_file, "r") as f:
                    content = (f.read() or "").strip()
                    if content == str(os.getpid()):
                        os.remove(pid_file)
        except Exception:
            pass

    atexit.register(_cleanup)
    try:
        signal.signal(signal.SIGTERM, lambda *_: (_cleanup(), os._exit(0)))
    except Exception:
        pass


