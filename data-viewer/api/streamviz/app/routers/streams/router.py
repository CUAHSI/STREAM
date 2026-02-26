from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.streams_service import StreamsError, StreamsService

router = APIRouter()
streams_service = StreamsService()


class StreamsLoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class StreamsLoginResponse(BaseModel):
    session_token: str
    expires_at: datetime


class StreamsLogoutResponse(BaseModel):
    success: bool


class StreamsDownloadRequest(BaseModel):
    gauges: list[str] = Field(..., min_length=1)
    start_date: datetime
    end_date: datetime
    water_quality_variables: list[str] = Field(default_factory=list)
    other_datasets: list[str] = Field(default_factory=list)


@router.post("/auth/login", response_model=StreamsLoginResponse)
async def streams_login(payload: StreamsLoginRequest):
    try:
        session = streams_service.login(payload.username, payload.password)
    except StreamsError as err:
        raise HTTPException(status_code=401, detail=str(err)) from err

    return StreamsLoginResponse(session_token=session.token, expires_at=session.expires_at)


@router.post("/auth/logout", response_model=StreamsLogoutResponse)
async def streams_logout(x_streams_session: str | None = Header(default=None)):
    if not x_streams_session:
        raise HTTPException(status_code=400, detail="Missing X-Streams-Session header.")
    streams_service.logout(x_streams_session)
    return StreamsLogoutResponse(success=True)


@router.get("/options")
async def streams_options():
    return streams_service.get_options()


@router.get("/gauges")
async def streams_gauges(
    x_streams_session: str | None = Header(default=None),
    max_features: int = Query(5000, ge=1, le=50000),
):
    try:
        return streams_service.get_gauges_geojson(x_streams_session, max_features=max_features)
    except StreamsError as err:
        raise HTTPException(status_code=401, detail=str(err)) from err
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Failed to fetch gauges: {err}") from err


@router.post("/download")
async def streams_download(payload: StreamsDownloadRequest, x_streams_session: str | None = Header(default=None)):
    if payload.end_date < payload.start_date:
        raise HTTPException(status_code=400, detail="end_date must be greater than or equal to start_date")

    try:
        zip_bytes = streams_service.build_download_zip(
            token=x_streams_session,
            gauges=payload.gauges,
            start_date=payload.start_date.astimezone(timezone.utc),
            end_date=payload.end_date.astimezone(timezone.utc),
            water_quality_variables=payload.water_quality_variables,
            other_datasets=payload.other_datasets,
        )
    except StreamsError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Failed to generate download: {err}") from err

    return StreamingResponse(
        iter([zip_bytes]),
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="streams-data.zip"'},
    )
