from __future__ import annotations

import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs

HYDROSHARE_S3_CREDENTIALS_URL = "https://www.hydroshare.org/hsapi/user/service/accounts/s3/"
HYDROSHARE_S3_ENDPOINT_URL = "https://s3.hydroshare.org"

SESSION_TTL_HOURS = 8

WATER_QUALITY_VARS: dict[str, list[str]] = {
    "Water Temperature": ["WTemp_C", "Flag_WTemp_C"],
    "Specific Conductance": ["SpC_uScm", "Flag_SpC_uScm"],
    "Dissolved Oxygen": ["DO_mgL", "Flag_DO_mgL"],
    "pH": ["pH", "Flag_pH"],
    "Turbidity": ["Turb_FNU", "Flag_Turb_FNU"],
    "NO3": ["NO3_mgNL", "Flag_NO3_mgNL"],
    "fDOM": ["fDOM_QSU", "Flag_fDOM_QSU", "fDOM_RFU", "Flag_fDOM_RFU"],
    "Chla": ["Chla_ugL", "Flag_Chla_ugL"],
    "PC": ["PC_RFU", "Flag_PC_RFU"],
}

DATASET_PATHS: dict[str, str] = {
    "Anthropogenic": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/dynamic_antropogenic.parquet",
    "gauges": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/gauges.parquet",
    "Grab Samples": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/grab_samples.parquet",
    "Land Use/Cover": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/lulc.parquet",
    "Streamflow": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/streamflow.parquet",
    "water_quality": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/water_quality.parquet",
    "Historical Meteorology": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/dynamic_historical_meteorology",
}

OTHER_DATASETS: list[str] = [
    "Streamflow",
    "Land Use/Cover",
    "Grab Samples",
    "Anthropogenic",
    "Historical Meteorology",
]

DATASET_TIME_COLUMNS: dict[str, str] = {
    "Streamflow": "DateTime",
    "Grab Samples": "DateTime",
    "Historical Meteorology": "time",
    "Land Use/Cover": "year",
    "Anthropogenic": "year",
}


class StreamsError(Exception):
    pass


@dataclass
class StreamsSession:
    token: str
    username: str
    s3_key: str
    s3_secret: str
    created_at: datetime

    @property
    def expires_at(self) -> datetime:
        return self.created_at + timedelta(hours=SESSION_TTL_HOURS)

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at


class StreamsService:
    def __init__(self):
        self._sessions: dict[str, StreamsSession] = {}

    def login(self, username: str, password: str) -> StreamsSession:
        # Sidecar-like conservative flow:
        # 1) GET service accounts
        # 2) If credentials are not directly present, POST once to create/get key+secret
        try:
            response_get = requests.get(HYDROSHARE_S3_CREDENTIALS_URL, auth=(username, password), timeout=20)
        except requests.RequestException as err:
            raise StreamsError(f"Unable to reach HydroShare: {err}") from err

        if response_get.status_code == 200:
            try:
                creds = response_get.json()
                s3_key, s3_secret = self._extract_s3_credentials(creds)
                return self._create_session(username, s3_key, s3_secret)
            except StreamsError:
                # Expected for GET payloads that expose key IDs without secrets.
                pass

        if response_get.status_code in (401, 403):
            raise StreamsError(f"HydroShare authentication failed (status {response_get.status_code}).")

        try:
            response_post = requests.post(HYDROSHARE_S3_CREDENTIALS_URL, auth=(username, password), timeout=20)
        except requests.RequestException as err:
            raise StreamsError(f"Unable to reach HydroShare: {err}") from err

        if response_post.status_code not in (200, 201):
            raise StreamsError(f"HydroShare authentication failed (status {response_post.status_code}).")

        try:
            creds = response_post.json()
            s3_key, s3_secret = self._extract_s3_credentials(creds)
        except Exception as err:
            raise StreamsError(f"HydroShare S3 credential parse failed: {err}") from err

        return self._create_session(username, s3_key, s3_secret)

    def _create_session(self, username: str, s3_key: str, s3_secret: str) -> StreamsSession:
        token = str(uuid4())
        session = StreamsSession(
            token=token,
            username=username,
            s3_key=s3_key,
            s3_secret=s3_secret,
            created_at=datetime.now(timezone.utc),
        )
        self._sessions[token] = session
        return session

    @staticmethod
    def _extract_s3_credentials(payload: dict[str, Any]) -> tuple[str, str]:
        # Legacy/simple payload shape:
        # {"access_key": "...", "secret_key": "..."}
        if isinstance(payload, dict):
            if payload.get("access_key") and payload.get("secret_key"):
                return payload["access_key"], payload["secret_key"]
            if payload.get("access_key") and payload.get("secret_access_key"):
                return payload["access_key"], payload["secret_access_key"]
            if payload.get("key") and payload.get("secret"):
                return payload["key"], payload["secret"]

            # Current HydroShare shape:
            # {"service_accounts": [{"access_key": "...", "secret_key": "...", ...}, ...]}
            accounts = payload.get("service_accounts")
            if isinstance(accounts, list) and accounts:
                for account in accounts:
                    if not isinstance(account, dict):
                        continue
                    if account.get("access_key") and account.get("secret_key"):
                        return account["access_key"], account["secret_key"]
                    if account.get("access_key") and account.get("secret_access_key"):
                        return account["access_key"], account["secret_access_key"]
                    if account.get("key") and account.get("secret"):
                        return account["key"], account["secret"]

            # Some responses may nest credentials under an object key.
            for value in payload.values():
                if isinstance(value, dict):
                    try:
                        return StreamsService._extract_s3_credentials(value)
                    except StreamsError:
                        pass
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            try:
                                return StreamsService._extract_s3_credentials(item)
                            except StreamsError:
                                pass

        raise StreamsError("HydroShare S3 credentials response was malformed.")

    def logout(self, token: str) -> None:
        self._sessions.pop(token, None)

    def get_session(self, token: str) -> StreamsSession:
        if not token:
            raise StreamsError("Missing STREAMS session token.")

        self._evict_expired_sessions()
        session = self._sessions.get(token)
        if not session:
            raise StreamsError("Invalid or expired STREAMS session token.")
        return session

    def get_options(self) -> dict[str, Any]:
        return {
            "water_quality_variables": WATER_QUALITY_VARS,
            "other_datasets": OTHER_DATASETS,
            "session_ttl_hours": SESSION_TTL_HOURS,
        }

    def get_gauges_geojson(self, token: str, max_features: int = 25000) -> dict[str, Any]:
        session = self.get_session(token)
        fs = self._build_s3_filesystem(session)

        table = pq.read_table(DATASET_PATHS["gauges"], filesystem=fs)
        gauges = table.to_pandas()

        if "latitude" not in gauges.columns or "longitude" not in gauges.columns:
            raise StreamsError("Gauge dataset does not include latitude/longitude columns.")

        if max_features > 0:
            gauges = gauges.head(max_features)

        features = []
        for _, row in gauges.iterrows():
            properties = {
                k: self._serialize_value(v) for k, v in row.items() if k not in {"latitude", "longitude", "geometry"}
            }
            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(row["longitude"]), float(row["latitude"])],
                    },
                    "properties": properties,
                }
            )

        return {"type": "FeatureCollection", "features": features}

    def build_download_zip(
        self,
        token: str,
        gauges: list[str],
        start_date: datetime,
        end_date: datetime,
        water_quality_variables: list[str],
        other_datasets: list[str],
    ) -> bytes:
        session = self.get_session(token)
        fs = self._build_s3_filesystem(session)

        selected_wq_columns = ["DateTime"]
        for label in water_quality_variables:
            if label not in WATER_QUALITY_VARS:
                raise StreamsError(f"Unknown water quality variable: {label}")
            selected_wq_columns.extend(WATER_QUALITY_VARS[label])

        output = BytesIO()
        with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for gauge in gauges:
                if len(selected_wq_columns) > 1:
                    wq_df = self._read_filtered_dataset(
                        fs=fs,
                        dataset_name="water_quality",
                        gauge=gauge,
                        start_date=start_date,
                        end_date=end_date,
                        columns=selected_wq_columns,
                    )
                    wq_filename = f"{self._build_output_filename(gauge, 'water_quality')}.csv"
                    zf.writestr(wq_filename, wq_df.to_csv(index=False))

                for dataset in other_datasets:
                    if dataset not in OTHER_DATASETS:
                        raise StreamsError(f"Unknown dataset selection: {dataset}")
                    dataset_df = self._read_filtered_dataset(
                        fs=fs,
                        dataset_name=dataset,
                        gauge=gauge,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    out_filename = f"{self._build_output_filename(gauge, dataset)}.csv"
                    zf.writestr(out_filename, dataset_df.to_csv(index=False))

        output.seek(0)
        return output.getvalue()

    def _read_filtered_dataset(
        self,
        fs: s3fs.S3FileSystem,
        dataset_name: str,
        gauge: str,
        start_date: datetime,
        end_date: datetime,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        path = DATASET_PATHS[dataset_name]
        filters = [("gauge", "=", gauge)]

        if dataset_name == "water_quality":
            time_col = "DateTime"
        else:
            time_col = DATASET_TIME_COLUMNS[dataset_name]

        start_value, end_value = self._build_time_range(
            fs=fs,
            path=path,
            time_col=time_col,
            start_date=start_date,
            end_date=end_date,
        )

        filters.extend([(time_col, ">=", start_value), (time_col, "<=", end_value)])
        table = pq.read_table(path, filesystem=fs, columns=columns, filters=filters)
        return table.to_pandas()

    def _build_s3_filesystem(self, session: StreamsSession) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            key=session.s3_key,
            secret=session.s3_secret,
            endpoint_url=HYDROSHARE_S3_ENDPOINT_URL,
        )

    def _evict_expired_sessions(self) -> None:
        expired_tokens = [token for token, session in self._sessions.items() if session.is_expired()]
        for token in expired_tokens:
            del self._sessions[token]

    @staticmethod
    def _build_output_filename(gauge_label: str, variable_label: str) -> str:
        var_label = variable_label.replace("/", "-").replace(" ", "-").lower()
        gauge_label = "-".join(gauge_label.split("-")[1:]) if "-" in gauge_label else gauge_label
        return f"{gauge_label}-{var_label}"

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return value

    @staticmethod
    def _build_time_range(
        fs: s3fs.S3FileSystem,
        path: str,
        time_col: str,
        start_date: datetime,
        end_date: datetime,
    ) -> tuple[Any, Any]:
        try:
            schema = pq.ParquetFile(path, filesystem=fs).schema_arrow
            field = schema.field(time_col)
            dtype = field.type
        except Exception:
            dtype = None

        if time_col == "year":
            if dtype is not None and pa.types.is_timestamp(dtype):
                return (
                    StreamsService._normalize_filter_timestamp(start_date),
                    StreamsService._normalize_filter_timestamp(end_date),
                )
            if dtype is not None and pa.types.is_string(dtype):
                return (str(start_date.year), str(end_date.year))
            return (int(start_date.year), int(end_date.year))

        return (
            StreamsService._normalize_filter_timestamp(start_date),
            StreamsService._normalize_filter_timestamp(end_date),
        )

    @staticmethod
    def _normalize_filter_timestamp(value: datetime) -> datetime | pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts
