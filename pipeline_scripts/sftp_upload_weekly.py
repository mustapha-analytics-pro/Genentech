"""Mirror E:/Projects/dcm/weekly_20260422_to_20260428/_stitched/ to a Genentech SFTP.

Usage:
    python sftp_upload_weekly.py <remote_dir> [--env dev|prod]
e.g.:
    python sftp_upload_weekly.py weekly_20260422_to_20260428 --env dev
    python sftp_upload_weekly.py dcm/weekly_20260422_to_20260428 --env prod

Default env is dev. Resumes partial uploads (skips files whose remote size
already matches local). The remote dir is created if missing.
"""

from __future__ import annotations

import argparse
import os
import posixpath
import sys
import time
from pathlib import Path

import paramiko

LOCAL_ROOT = Path(r"E:/Projects/dcm/weekly_20260422_to_20260428/_stitched")
KEY = os.path.expanduser("~/.ssh/genentech_cmg_oasis_id_rsa")

ENVS = {
    "dev":  ("sftp.cmgoasis.dev.gene.com", "cmg_oasis_dev_improvado_user"),
    "prod": ("sftp-cmgoasis.gene.com",     "cmg_oasis_prod_improvado_user"),
}

CHUNK = 32 * 1024 * 1024   # 32 MB transfer window
PROGRESS_EVERY = 256 * 1024 * 1024  # log every 256 MB


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    parts = [p for p in remote_dir.split("/") if p]
    cur = ""
    for p in parts:
        cur = posixpath.join(cur, p) if cur else p
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)


def remote_size(sftp: paramiko.SFTPClient, remote_path: str) -> int | None:
    try:
        return sftp.stat(remote_path).st_size
    except FileNotFoundError:
        return None


def upload_one(sftp: paramiko.SFTPClient, local: Path, remote_path: str) -> None:
    local_size = local.stat().st_size
    rsz = remote_size(sftp, remote_path)
    if rsz == local_size:
        print(f"  SKIP  {local.name}  (remote already {rsz/1e9:.2f} GB)")
        return
    if rsz is not None:
        print(f"  REPLACE  {local.name}  (remote {rsz/1e9:.2f} GB != local {local_size/1e9:.2f} GB)")
    else:
        print(f"  UPLOAD  {local.name}  ({local_size/1e9:.2f} GB)")

    t0 = time.monotonic()
    sent = 0
    last_log = 0
    with open(local, "rb") as src, sftp.open(remote_path, "wb") as dst:
        dst.set_pipelined(True)
        while True:
            buf = src.read(CHUNK)
            if not buf:
                break
            dst.write(buf)
            sent += len(buf)
            if sent - last_log >= PROGRESS_EVERY:
                elapsed = time.monotonic() - t0
                mbps = (sent / 1e6) / max(elapsed, 0.001)
                pct = sent / local_size * 100 if local_size else 100
                print(f"    {local.name}: {sent/1e9:.2f} / {local_size/1e9:.2f} GB"
                      f"  ({pct:5.1f}%)  {mbps:6.1f} MB/s  ({elapsed:.0f}s)", flush=True)
                last_log = sent
    elapsed = time.monotonic() - t0
    mbps = (sent / 1e6) / max(elapsed, 0.001)
    print(f"  DONE  {local.name}  in {elapsed:.0f}s  ({mbps:.1f} MB/s)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("remote_dir")
    ap.add_argument("--env", choices=list(ENVS), default="dev")
    args = ap.parse_args()

    remote_dir = args.remote_dir.strip("/")
    host, user = ENVS[args.env]

    if not LOCAL_ROOT.is_dir():
        print(f"local missing: {LOCAL_ROOT}", file=sys.stderr)
        return 1

    locals_ = sorted(p for p in LOCAL_ROOT.iterdir() if p.is_file())
    total_bytes = sum(p.stat().st_size for p in locals_)
    print(f"Local : {LOCAL_ROOT}  ({len(locals_)} files, {total_bytes/1e9:.2f} GB)")
    print(f"Env   : {args.env}")
    print(f"Remote: {user}@{host}:{remote_dir}/")

    pkey = paramiko.RSAKey.from_private_key_file(KEY)
    transport = paramiko.Transport((host, 22))
    transport.connect(username=user, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(t=transport)
    try:
        ensure_remote_dir(sftp, remote_dir)
        for local in locals_:
            remote_path = posixpath.join(remote_dir, local.name)
            upload_one(sftp, local, remote_path)
        print("\nALL DONE")
    finally:
        sftp.close()
        transport.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
