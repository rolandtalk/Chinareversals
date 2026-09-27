import pandas as pd

import app as app_module


def test_code_to_ticker():
    assert app_module.code_to_ticker("600519") == "600519.SS"
    assert app_module.code_to_ticker("688082") == "688082.SS"
    assert app_module.code_to_ticker("000333") == "000333.SZ"
    assert app_module.code_to_ticker("920982") == "920982.BJ"
    assert app_module.code_to_ticker("399001") == "399001.SZ"
    assert app_module.code_to_ticker("01888", "HK") == "1888.HK"
    assert app_module.code_to_ticker("00006", "HK") == "0006.HK"


def test_stock_research_links_are_exchange_aware():
    assert app_module.stock_research_links("600519", "CN", "600519.SS") == {
        "tradingview_url": "https://www.tradingview.com/symbols/SSE-600519/?timeframe=1M",
    }
    assert app_module.stock_research_links("000333", "CN", "000333.SZ") == {
        "tradingview_url": "https://www.tradingview.com/symbols/SZSE-000333/?timeframe=1M",
    }
    assert app_module.stock_research_links("00006", "HK", "0006.HK") == {
        "tradingview_url": "https://www.tradingview.com/symbols/HKEX-6/?timeframe=1M",
    }
    assert app_module.stock_research_links("920982", "CN", "920982.BJ") == {
        "tradingview_url": "https://www.tradingview.com/search/?query=920982",
    }


def test_bundled_universe_has_two_markets_fourteen_categories_and_611_rows():
    rows = app_module._load_bundled_stocks()
    assert len(rows) == 611
    assert len({row["category"] for row in rows}) == 14
    assert {row["market"] for row in rows} == {"CN", "HK"}
    assert sum(row["market"] == "CN" for row in rows) == 473
    assert sum(row["market"] == "HK" for row in rows) == 138
    assert sum(row["category"] == "創新藥" for row in rows) == 6
    assert sum(row["category"] == "創新藥產業鏈" for row in rows) == 8
    assert sum(row["category"] == "世界第一" for row in rows) == 50
    assert sum(row["category"] == "PCB產業鏈" for row in rows) == 5
    assert sum(row["category"] == "申萬一級行業Top10" for row in rows) == 243
    assert {
        row["ticker"] for row in rows if row["category"] == "PCB產業鏈"
    } == {"002436.SZ", "601208.SS", "301217.SZ", "688300.SS", "002463.SZ"}
    assert len({row["ticker"] for row in rows}) == len(rows)
    assert all(row["ticker"] == app_module.code_to_ticker(row["code"], row["market"]) for row in rows)


def test_calculate_metrics():
    index = pd.date_range("2026-01-01", periods=30, freq="D")
    prices = [10 + i * 0.1 for i in range(20)] + [12, 11.5, 11, 10.5, 10.8, 11.1, 11.4, 11.8, 12.1, 12.5]
    result = app_module.calculate_metrics(pd.DataFrame({"Close": prices}, index=index))
    assert result is not None
    assert result["price"] == 12.5
    assert result["gg"] > 0
    assert result["d60"] is None


def test_calculate_metrics_includes_60_session_return():
    index = pd.date_range("2026-01-01", periods=70, freq="D")
    prices = [10 + i * 0.1 for i in range(60)] + [16, 15.5, 15, 14.5, 14.8, 15.1, 15.4, 15.8, 16.1, 16.5]
    result = app_module.calculate_metrics(pd.DataFrame({"Close": prices}, index=index))
    assert result is not None
    assert result["d60"] == 51.38


def test_health_and_home():
    client = app_module.app.test_client()
    health = client.get("/health")
    assert health.status_code == 200
    assert health.get_json()["app"] == "Chinareversals"
    home = client.get("/")
    assert home.status_code == 200
    assert b"Chinareversals" in home.data
    assert b'href="https://chinareversals-production.up.railway.app"' in home.data
    assert b">Production</a>" in home.data
    assert b"View on TradingView" in home.data
    assert b"View on Eastmoney" not in home.data
    assert b'data-sort="category"' not in home.data
    assert b'data-sort="d60"' in home.data
    assert b'class="stock-column"' in home.data


def test_stock_api_includes_research_links():
    client = app_module.app.test_client()
    response = client.get("/api/stocks")
    assert response.status_code == 200
    stocks = response.get_json()["stocks"]
    assert stocks
    assert all("eastmoney_url" not in stock for stock in stocks)
    standard = [stock for stock in stocks if not stock["ticker"].endswith(".BJ")]
    assert all(stock["tradingview_url"].startswith("https://www.tradingview.com/symbols/") for stock in standard)
    assert all(stock["tradingview_url"].endswith("?timeframe=1M") for stock in standard)


def test_database_seed_and_status():
    rows, source = app_module.load_stocks()
    assert source in {"database", "google_sheet", "bundled_snapshot"}
    assert len(rows) == 611
    summary = app_module.database_summary()
    assert summary["connected"] is True
    assert summary["stocks"] == 611

    client = app_module.app.test_client()
    response = client.get("/api/database-status")
    assert response.status_code == 200
    assert response.get_json()["stocks"] == 611


def test_data_api_reads_completed_database_snapshot(monkeypatch):
    monkeypatch.setattr(
        app_module,
        "scan_stocks",
        lambda _stocks: (_ for _ in ()).throw(AssertionError("live scan called")),
    )
    client = app_module.app.test_client()
    response = client.get("/api/data?refresh=1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["cached"] is True
    assert len(payload["rows"]) == 611
    assert all("d60" in row for row in payload["rows"])
