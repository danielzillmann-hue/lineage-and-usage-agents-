from fastapi import APIRouter, HTTPException, Query

from app.models.run import BucketPreview
from app.services import gcs

router = APIRouter()


@router.get("", response_model=list[str])
def list_buckets() -> list[str]:
    return gcs.list_buckets()


@router.get("/{bucket}/preview", response_model=BucketPreview)
def preview_bucket(bucket: str, prefix: str = Query("")) -> BucketPreview:
    try:
        return gcs.preview(bucket=bucket, prefix=prefix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
