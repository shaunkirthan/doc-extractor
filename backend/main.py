from fastapi import FastAPI
from uuid import uuid4
import boto3
import os

app = FastAPI()
BUCKET = "pdf-extract-demo"
REGION = "us-east-1"
s3 = boto3.client("s3", region_name=REGION)

# Simple in-memory status tracker (can be replaced with DynamoDB)
STATUS = {}

@app.post("/upload")
def upload():
    doc_id = str(uuid4())
    key = f"uploads/{doc_id}.pdf"
    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": BUCKET, "Key": key, "ContentType": "application/pdf"},
        ExpiresIn=900,
    )
    STATUS[doc_id] = "pending"
    return {"doc_id": doc_id, "url": url}

@app.get("/results/{doc_id}")
def get_results(doc_id: str):
    if STATUS.get(doc_id) != "done":
        return {"status": "pending"}
    
    base = f"https://{BUCKET}.s3.{REGION}.amazonaws.com/results/{doc_id}"
    images = [f"{base}/images/page{i}.png" for i in range(1, 4)]  # example
    tables = [f"{base}/tables/table{i}.csv" for i in range(1, 3)]
    pdf = f"{base}/report.pdf"
    return {"status": "done", "images": images, "tables": tables, "pdf": pdf}

