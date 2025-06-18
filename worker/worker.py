#!/usr/bin/env python
import os, time, subprocess, json, tempfile
import boto3, requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

REGION  = os.environ["AWS_REGION"]
BUCKET  = os.environ["BUCKET"]
API     = os.environ["BACKEND_BASE"].rstrip("/")
s3      = boto3.client("s3", region_name=REGION)

LOCAL_IN  = Path("input_pdfs")
LOCAL_OUT = Path("output_sync")      # temp folder for uploads
LOCAL_IN.mkdir(exist_ok=True)
LOCAL_OUT.mkdir(exist_ok=True)

def list_new_uploads():
    """Return list of (doc_id, s3_key) that haven’t been processed yet."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="uploads/")
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        doc_id = Path(key).stem                # uploads/<doc_id>.pdf
        flag = LOCAL_IN / f".done_{doc_id}"
        if not flag.exists():
            yield doc_id, key

def run_extractor(local_pdf, doc_id):
    # ✂️  call your existing script exactly as you do now
    subprocess.run(["python", "extract_cli.py", local_pdf], check=True)
    subprocess.run(["python", "make_pdf.py",  doc_id],     check=True)

def sync_results_to_s3(doc_id):
    base_remote = f"results/{doc_id}"
    # raw imgs / csv already written by extract_cli.py
    subprocess.run(["aws","s3","sync",
                    f"output_images/{doc_id}", f"s3://{BUCKET}/{base_remote}/images"],
                   check=True)
    subprocess.run(["aws","s3","sync",
                    f"output_csv/{doc_id}",    f"s3://{BUCKET}/{base_remote}/tables"],
                   check=True)
    subprocess.run(["aws","s3","cp",
                    f"output_images/{doc_id}/report.pdf",
                    f"s3://{BUCKET}/{base_remote}/report.pdf"],
                   check=True)

def notify_backend(doc_id):
    url = f"{API}/internal/mark_done"
    requests.post(url, json={"doc_id": doc_id}, timeout=5)

def main_loop(poll_interval=5):
    seen = set()

    while True:
        print("🔄 Polling S3 for new PDFs...", flush=True)

        for doc_id, key in list_new_uploads():
            if doc_id in seen:
                continue

            print(f"📥 Processing {doc_id}")
            local_pdf = LOCAL_IN / f"{doc_id}.pdf"
            s3.download_file(Bucket=BUCKET, Key=key, Filename=str(local_pdf))

            try:
                run_extractor(local_pdf, doc_id)          # pass Path OK
                sync_results_to_s3(doc_id)
                notify_backend(doc_id)                 # or notify_backend
                (LOCAL_IN / f".done_{doc_id}").touch()
                seen.add(doc_id)                          # remember it
                print(f"✅ {doc_id} done")
            except Exception as e:
                print(f"❌ error on {doc_id}: {e}")

        time.sleep(poll_interval)
        

if __name__ == "__main__":
    main_loop(poll_interval=5)



