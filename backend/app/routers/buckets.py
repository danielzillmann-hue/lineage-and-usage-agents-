from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.config import get_settings
from app.models.run import BucketPreview, OracleConnection
from app.services import gcs
from app.services import oracle as ora

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


# ─── Demo defaults + Oracle connectivity test ─────────────────────────────


class DemoDefaults(BaseModel):
    oracle: OracleConnection
    bucket: str
    prefix: str
    outputs_prefix: str


class TestConnectionResponse(BaseModel):
    ok: bool
    schema_name: str | None = None
    table_count: int | None = None
    pipeline_runs: int | None = None
    error: str | None = None


# Mounted at module level (router prefix is /api/buckets) — but defaults+test
# don't fit there cleanly. We re-export them under a different router below.
_demo_router = APIRouter()


@_demo_router.get("/demo-defaults", response_model=DemoDefaults)
def demo_defaults() -> DemoDefaults:
    s = get_settings()
    return DemoDefaults(
        oracle=OracleConnection(
            host=s.demo_db_host, port=s.demo_db_port, service=s.demo_db_service,
            user=s.demo_db_user, password=s.demo_db_password,
        ),
        bucket=s.demo_etl_bucket,
        prefix=s.demo_etl_prefix,
        outputs_prefix=s.demo_outputs_prefix,
    )


@_demo_router.post("/oracle/test", response_model=TestConnectionResponse)
def test_oracle(conn: OracleConnection) -> TestConnectionResponse:
    try:
        snap = ora.snapshot(ora.OracleConn(
            host=conn.host, port=conn.port, service=conn.service,
            user=conn.user, password=conn.password,
        ))
        return TestConnectionResponse(
            ok=True,
            schema_name=snap.schema,
            table_count=len(snap.tables),
            pipeline_runs=len(snap.pipeline_runs),
        )
    except Exception as e:  # noqa: BLE001
        return TestConnectionResponse(ok=False, error=str(e))


def get_demo_router() -> APIRouter:
    return _demo_router
