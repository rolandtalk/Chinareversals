import pandas as pd

import app as app_module


def test_code_to_ticker():
    assert app_module.code_to_ticker("600519") == "600519.SS"
    assert app_module.code_to_ticker("688082") == "688082.SS"
    assert app_module.code_to_ticker("000333") == "000333.SZ"
    assert app_module.code_to_ticker("399001") == "399001.SZ"


def test_bundled_universe_has_nine_categories_and_170_rows():
    rows = app_module._load_bundled_stocks()
    assert len(rows) == 170
    assert len({row["category"] for row in rows}) == 9
    assert all(row["ticker"] == app_module.code_to_ticker(row["code"]) for row in rows)


def test_calculate_metrics():
    index = pd.date_range("2026-01-01", periods=30, freq="D")
    prices = [10 + i * 0.1 for i in range(20)] + [12, 11.5, 11, 10.5, 10.8, 11.1, 11.4, 11.8, 12.1, 12.5]
    result = app_module.calculate_metrics(pd.DataFrame({"Close": prices}, index=index))
    assert result is not None
    assert result["price"] == 12.5
    assert result["gg"] > 0


def test_health_and_home():
    client = app_module.app.test_client()
    health = client.get("/health")
    assert health.status_code == 200
    assert health.get_json()["app"] == "Chinareversals"
    home = client.get("/")
    assert home.status_code == 200
    assert b"Chinareversals" in home.data


def test_database_seed_and_status():
    rows, source = app_module.load_stocks()
    assert source in {"google_sheet", "bundled_snapshot"}
    assert len(rows) == 170
    summary = app_module.database_summary()
    assert summary["connected"] is True
    assert summary["stocks"] == 170

    client = app_module.app.test_client()
    response = client.get("/api/database-status")
    assert response.status_code == 200
    assert response.get_json()["stocks"] == 170
