import subprocess
import sys
import threading


_install_lock = threading.Lock()
_install_done = False


def ensure_playwright_chromium(status_callback=None) -> None:
    """
    Ensure Playwright Chromium browser binaries are installed.
    This is required on Streamlit Cloud where only Python deps are installed.
    """
    global _install_done
    if _install_done:
        return

    if status_callback is None:
        status_callback = lambda _: None

    with _install_lock:
        if _install_done:
            return
        status_callback("🔧 Đang kiểm tra/cài Chromium cho Playwright...")
        proc = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            stderr_tail = (proc.stderr or "").strip()[-1200:]
            stdout_tail = (proc.stdout or "").strip()[-1200:]
            raise RuntimeError(
                "Không thể cài Chromium cho Playwright. "
                f"stdout: {stdout_tail} | stderr: {stderr_tail}"
            )
        _install_done = True
        status_callback("✅ Chromium cho Playwright đã sẵn sàng.")
