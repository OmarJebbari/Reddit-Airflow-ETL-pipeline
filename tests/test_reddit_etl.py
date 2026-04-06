import os
from types import SimpleNamespace

import pandas as pd
import pytest

from etls import reddit_etl


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_extract_reddit_data_scraping_parses_mocked_payload(monkeypatch, reddit_post_payload):
    payload = {
        "data": {
            "children": [{"data": reddit_post_payload}],
            "after": None,
        }
    }

    monkeypatch.setattr(
        reddit_etl,
        "_fetch_reddit_page",
        lambda **kwargs: DummyResponse(payload),
    )

    rows = reddit_etl.extract_reddit_data_scraping("python", max_posts=1)

    assert len(rows) == 1
    assert rows[0]["id"] == "abc123"
    assert rows[0]["subreddit"] == "python"


def test_transform_handles_missing_fields_gracefully():
    raw = [{"id": "x1", "created_utc": 1710000000, "edited": None}]

    df = reddit_etl.transform_reddit_data(raw)

    assert df.loc[0, "author"] == "unknown"
    assert df.loc[0, "title"] == ""
    assert df.loc[0, "score"] == 0
    assert df.loc[0, "num_comments"] == 0
    assert bool(df.loc[0, "spoiler"]) is False


def test_load_data_to_parquet_minio_uses_expected_bucket_and_key(monkeypatch):
    os.environ["MINIO_ENDPOINT"] = "http://minio:9000"
    os.environ["MINIO_ACCESS_KEY"] = "key"
    os.environ["MINIO_SECRET_KEY"] = "secret"
    os.environ["MINIO_BRONZE_BUCKET"] = "bronze"
    os.environ["MINIO_BRONZE_PREFIX"] = "reddit"

    calls = {}

    class FakeFS:
        def exists(self, bucket):
            calls["exists_bucket"] = bucket
            return False

        def mkdir(self, bucket):
            calls["created_bucket"] = bucket

    monkeypatch.setattr(reddit_etl.s3fs, "S3FileSystem", lambda **kwargs: FakeFS())

    def fake_to_parquet(self, path, index, filesystem):
        calls["parquet_path"] = path
        calls["index"] = index
        calls["filesystem"] = filesystem

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=False)

    df = pd.DataFrame([{"id": "1"}])
    out = reddit_etl.load_data_to_parquet_minio(df, "reddit_20260406")

    assert calls["exists_bucket"] == "bronze"
    assert calls["created_bucket"] == "bronze"
    assert out == "s3://bronze/reddit/reddit_20260406.parquet"
    assert calls["parquet_path"] == out


def test_build_postgres_url_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_HOST", "h")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "d")

    assert reddit_etl._build_postgres_url() == "postgresql+psycopg2://u:p@h:5432/d"


def test_load_latest_bronze_parquet_selects_by_last_modified(monkeypatch):
    os.environ["MINIO_ENDPOINT"] = "http://minio:9000"
    os.environ["MINIO_ACCESS_KEY"] = "key"
    os.environ["MINIO_SECRET_KEY"] = "secret"
    os.environ["MINIO_BRONZE_BUCKET"] = "bronze"
    os.environ["MINIO_BRONZE_PREFIX"] = "reddit"

    captured = {}

    class FakeFS:
        def glob(self, path):
            captured["glob_path"] = path
            return [
                "s3://bronze/reddit/a.parquet",
                "s3://bronze/reddit/b.parquet",
            ]

        def info(self, path):
            return {"LastModified": "2026-04-06T12:00:00" if path.endswith("b.parquet") else "2026-04-06T10:00:00"}

    monkeypatch.setattr(reddit_etl.s3fs, "S3FileSystem", lambda **kwargs: FakeFS())

    monkeypatch.setattr(reddit_etl.pd, "read_parquet", lambda path, filesystem: pd.DataFrame([{"id": "1"}]))

    class FakeEngine:
        def dispose(self):
            captured["disposed"] = True

    monkeypatch.setattr(reddit_etl, "create_engine", lambda url: FakeEngine())

    def fake_to_sql(self, table_name, engine, if_exists, index, method, chunksize):
        captured["table"] = table_name
        captured["if_exists"] = if_exists

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    table_name = reddit_etl.load_latest_bronze_parquet_to_postgres("reddit_gold")

    assert captured["glob_path"] == "s3://bronze/reddit/*.parquet"
    assert captured["table"] == "reddit_gold"
    assert captured["if_exists"] == "replace"
    assert captured["disposed"] is True
    assert table_name == "reddit_gold"
