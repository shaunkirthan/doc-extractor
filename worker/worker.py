#!/usr/bin/env python
"""
worker.py – watches the S3 uploads/ prefix, downloads new PDFs,
runs extract_cli.py + make_pdf.py, syncs the results back to S3,
and notifies the FastAPI backend.

Put this file in    <repo_root>/worker/worker.py
"""

import os, time, subprocess, json
from pathlib import Path

import boto3, requests
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# 0. Load configuration                                                       #
# --------------------------------------------------------------------------- #
load_dotenv()                                              # reads .env in repo root

REGION   = os.environ["AWS_REGION"]        # e.g. us-east-1
BUCKET   = os.environ["BUCKET"]            # e.g. pdf-extract-demo
API_BASE = os.environ["BACKEND_BASE"].rstrip("/")  # http://localhost:8000

s3   = boto3.client("s3", region_name=REGION)

# repo_root  = one level **above** the folder this script lives in
REPO_ROOT   = Path(__file__).resolve().parent.parent
EXTRACT_CLI = REPO_ROOT / "extract_cli.py"
MAKE_PDF    = REPO_ROOT / "make_pdf.py"

LOCAL_IN    = REPO_ROOT / "input_pdfs"
LOCAL_OUT   = REPO_ROOT / "output_sync"       # just a temp staging dir
LOCAL_IN.mkdir(exist_ok=True)
LOCAL_OUT.mkdir(exist_ok=True)

# --------------------------------------------------------------------------- #
# 1. Helpers                                                                  #
# --------------------------------------------------------------------------- #
def list_new_uploads():
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="uploads/")
    for obj in resp.get("Contents", []):
        key = obj["Key"]

        # --- NEW GUARD -------------------------------------------------
        if obj["Size"] < 1024:  # skip anything <1 KB
            print(f"⚠️  Skip {key}: {obj['Size']} bytes")
            continue
        # ---------------------------------------------------------------

        if not key.lower().endswith(".pdf"):
            continue
        doc_id = Path(key).stem
        done_flag = LOCAL_IN / f".done_{doc_id}"
        if not done_flag.exists():
            yield doc_id, key



def run_extractor(local_pdf: Path, doc_id: str):
    """Call extract_cli.py then make_pdf.py *from the repo root*."""
    subprocess.run(
        ["python", str(EXTRACT_CLI), str(local_pdf)],
        cwd=REPO_ROOT,
        check=True,
    )
    subprocess.run(
        ["python", str(MAKE_PDF),  doc_id],
        cwd=REPO_ROOT,
        check=True,
    )

def sync_results_to_s3(doc_id: str):
    base_remote = f"results/{doc_id}"
    subprocess.run(
        ["aws","s3","sync",
         f"output_images/{doc_id}", f"s3://{BUCKET}/{base_remote}/images"],
        cwd=REPO_ROOT,
        check=True,
    )
    subprocess.run(
        ["aws","s3","sync",
         f"output_csv/{doc_id}", f"s3://{BUCKET}/{base_remote}/tables"],
        cwd=REPO_ROOT,
        check=True,
    )
    subprocess.run(
        ["aws","s3","cp",
         f"output_images/{doc_id}/report.pdf",
         f"s3://{BUCKET}/{base_remote}/report.pdf"],
        cwd=REPO_ROOT,
        check=True,
    )

def notify_backend(doc_id: str):
    """POST {doc_id} to /internal/mark_done so FastAPI flips status → done."""
    url = f"{API_BASE}/internal/mark_done"
    requests.post(url, json={"doc_id": doc_id}, timeout=5)

# --------------------------------------------------------------------------- #
# 2. Main polling loop                                                        #
# --------------------------------------------------------------------------- #
def main_loop(poll_interval: int = 5):
    print(f"👀 Worker started. Watching s3://{BUCKET}/uploads/ every {poll_interval}s")
    while True:
        for doc_id, key in list_new_uploads():
            print(f"📥  Found new upload: {doc_id}")
            local_pdf = LOCAL_IN / f"{doc_id}.pdf"
            s3.download_file(BUCKET, key, str(local_pdf))

            try:
                run_extractor(local_pdf, doc_id)
                sync_results_to_s3(doc_id)
                notify_backend(doc_id)
                (LOCAL_IN / f".done_{doc_id}").touch()      # create flag
                print(f"✅  {doc_id} processed & uploaded")
            except subprocess.CalledProcessError as e:
                print(f"❌  Extractor failed for {doc_id}: {e}")
            except Exception as e:
                print(f"❌  Unexpected error for {doc_id}: {e}")

        time.sleep(poll_interval)

if __name__ == "__main__":
    main_loop(poll_interval=5)
