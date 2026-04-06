import pandas as pd

from pipelines import reddit_pipeline as rp


def test_reddit_pipeline_invokes_extract_transform_and_load(monkeypatch):
    calls = {"extract": [], "csv": None, "parquet": None}

    def fake_extract(subreddit, time_filter, limit, max_posts):
        calls["extract"].append((subreddit, time_filter, limit, max_posts))
        return [{"id": f"{subreddit}-1"}]

    def fake_transform(raw_data):
        assert len(raw_data) == 2
        return pd.DataFrame(raw_data)

    def fake_load_csv(df, path):
        calls["csv"] = path

    def fake_load_parquet(df, file_name):
        calls["parquet"] = file_name
        return f"s3://bronze/reddit/{file_name}.parquet"

    monkeypatch.setattr(rp, "extract_reddit_data_scraping", fake_extract)
    monkeypatch.setattr(rp, "transform_reddit_data", fake_transform)
    monkeypatch.setattr(rp, "load_data_to_csv", fake_load_csv)
    monkeypatch.setattr(rp, "load_data_to_parquet_minio", fake_load_parquet)
    monkeypatch.setattr(rp, "OUTPUT_PATH", "/tmp/out")

    output_path = rp.reddit_pipeline(
        file_name="reddit_test",
        subreddit=["python", "aws"],
        time_filter="year",
        limit=100,
        max_posts=200,
    )

    assert calls["extract"] == [
        ("python", "year", 100, 200),
        ("aws", "year", 100, 200),
    ]
    assert calls["csv"] == "/tmp/out/reddit_test.csv"
    assert calls["parquet"] == "reddit_test"
    assert output_path == "/tmp/out/reddit_test.csv"
