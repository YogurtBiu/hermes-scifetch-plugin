"""OS file ownership and read-only process checks for the plugin outbox."""

from __future__ import annotations

import errno
import ntpath
import os
import sys
from pathlib import Path
from typing import cast

if sys.platform == "win32":
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class _FileInformation(ctypes.Structure):
        _fields_ = [
            ("attributes", wintypes.DWORD),
            ("created", wintypes.FILETIME),
            ("accessed", wintypes.FILETIME),
            ("written", wintypes.FILETIME),
            ("volume", wintypes.DWORD),
            ("size_high", wintypes.DWORD),
            ("size_low", wintypes.DWORD),
            ("links", wintypes.DWORD),
            ("index_high", wintypes.DWORD),
            ("index_low", wintypes.DWORD),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    _kernel32.CreateFileW.restype = wintypes.HANDLE
    _kernel32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_FileInformation),
    ]
    _kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel32.CloseHandle.restype = wintypes.BOOL
    _kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _kernel32.OpenProcess.restype = wintypes.HANDLE
    _kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    _kernel32.WaitForSingleObject.restype = wintypes.DWORD
else:
    import fcntl


def open_private_file(path: Path, flags: int) -> int:
    """Open a regular leaf without following a symlink/reparse point."""
    if sys.platform == "win32":
        access_mode = flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
        access = 0xC0000000 if access_mode == os.O_RDWR else 0x40000000
        if access_mode == os.O_RDONLY:
            access = 0x80000000
        disposition = 1 if flags & os.O_EXCL else (4 if flags & os.O_CREAT else 3)
        # Share reads/writes for SQLite, but deny delete/replace while this handle lives.
        handle = _kernel32.CreateFileW(str(path), access, 3, None, disposition, 0x00200080, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            information = _FileInformation()
            if not _kernel32.GetFileInformationByHandle(handle, ctypes.byref(information)):
                raise ctypes.WinError(ctypes.get_last_error())
            # FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT.
            if information.attributes & 0x410:
                raise OSError(errno.ELOOP, "Outbox leaf must be a regular file", str(path))
            fd = msvcrt.open_osfhandle(
                cast(int, handle), access_mode | os.O_BINARY | os.O_NOINHERIT
            )
        except BaseException:
            _kernel32.CloseHandle(handle)
            raise
        return fd
    else:
        return os.open(path, flags | os.O_NOFOLLOW, 0o600)


def lock_sender(fd: int) -> None:
    """Acquire an OS-backed, nonblocking sender lock on a fixed file region."""
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise BlockingIOError(exc.errno, "Installation sender is already owned") from exc
            raise
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def unlock_sender(fd: int) -> None:
    """Release exactly the region acquired by lock_sender."""
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


def canonical_path(path: Path) -> str:
    """Use the host's canonical spelling for a persisted ledger binding."""
    return str(path.resolve())


def staging_path(path: Path) -> Path:
    """Represent a Windows staging path without the ordinary MAX_PATH limit."""
    if sys.platform != "win32":
        return path
    value = ntpath.normpath(str(path))
    if value.startswith("\\\\.\\"):
        raise ValueError("Staging requires a filesystem path")
    if value.startswith("\\\\?\\"):
        remainder = value[4:]
        if remainder.upper().startswith("UNC\\"):
            drive, tail = ntpath.splitdrive("\\\\" + remainder[4:])
            if drive.count("\\") < 3 or (tail and not tail.startswith("\\")):
                raise ValueError("Staging requires an absolute UNC path")
        elif not (
            len(remainder) >= 3
            and remainder[0].lower() in "abcdefghijklmnopqrstuvwxyz"
            and remainder[1:3] == ":\\"
        ):
            raise ValueError("Staging requires a drive or UNC filesystem path")
        return Path(value)
    drive, tail = ntpath.splitdrive(value)
    if not drive or not tail.startswith("\\"):
        value = ntpath.normpath(ntpath.abspath(value))
        drive, tail = ntpath.splitdrive(value)
    if value.startswith("\\\\") and drive.count("\\") >= 3:
        return Path("\\\\?\\UNC\\" + value[2:])
    if len(drive) == 2 and drive[1] == ":" and tail.startswith("\\"):
        return Path("\\\\?\\" + value)
    raise ValueError("Staging requires an absolute filesystem path")


def process_exited(process_id: int) -> bool:
    """Return true only when the OS confirms that a different process exited."""
    if process_id <= 0 or process_id == os.getpid():
        return False
    if sys.platform == "win32":
        if process_id > 0xFFFFFFFF:
            return False
        # SYNCHRONIZE permits a zero-time wait, with no terminate/suspend/write rights.
        handle = _kernel32.OpenProcess(0x00100000, False, process_id)
        if not handle:
            # A positive PID that no longer exists is ERROR_INVALID_PARAMETER.
            return ctypes.get_last_error() == 87
        try:
            return bool(_kernel32.WaitForSingleObject(handle, 0) == 0)
        finally:
            _kernel32.CloseHandle(handle)
    else:
        try:
            os.kill(process_id, 0)
        except ProcessLookupError:
            return True
        except (OSError, OverflowError):
            pass
        return False
