from fastapi import FastAPI, File, UploadFile

app = FastAPI(
    title="Basic File Ingestion with Celery",
)


@app.post(
    "/ingest"
)
async def ingest_endpoint(
  file: UploadFile = File(...)  
):
    return {"name": file.filename, "mime": file.content_type}
