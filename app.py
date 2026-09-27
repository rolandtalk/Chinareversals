"""China and Hong Kong MA3 reversal scanner."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import threading
import urllib.parse
import urllib.request

from flask import Flask, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import yfinance as yf
from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, and_, create_engine, func, inspect, select, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "data" / "stock_universe.json"

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000").rstrip("/")
PRODUCTION_URL = os.getenv(
    "PRODUCTION_URL", "https://chinareversals-production.up.railway.app"
).rstrip("/")
GITHUB_REPO_URL = os.getenv(
    "GITHUB_REPO_URL", "https://github.com/rolandtalk/Chinareversals"
)
GOOGLE_SHEET_ID = os.getenv(
    "GOOGLE_SHEET_ID", "1wrwDcv5EJRgvhgEDWY1p5Gt5NIbeBRtB2J3crKHDL2g"
)
GOOGLE_SHEET_URL = (
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit"
)
SOURCE_SHEET_NAME = os.getenv("SOURCE_SHEET_NAME", "股票清單")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "45"))

RAW_DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'chinareversals.db'}")
if RAW_DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif RAW_DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = RAW_DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
else:
    DATABASE_URL = RAW_DATABASE_URL


class Base(DeclarativeBase):
    pass


class Stock(Base):
    __tablename__ = "stocks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(12), unique=True, index=True)
    ticker: Mapped[str] = mapped_column(String(24), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(160))
    category: Mapped[str] = mapped_column(String(80), index=True)
    market: Mapped[str] = mapped_column(String(12), default="CN")
    as_of: Mapped[str] = mapped_column(String(16), default="")
    source: Mapped[str] = mapped_column(Text, default="")
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    results: Mapped[list["ScanResult"]] = relationship(back_populates="stock", cascade="all, delete-orphan")


class ScanRun(Base):
    __tablename__ = "scan_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    source_mode: Mapped[str] = mapped_column(String(40), default="bundled_snapshot")
    total_count: Mapped[int] = mapped_column(Integer, default=0)
    available_count: Mapped[int] = mapped_column(Integer, default=0)
    results: Mapped[list["ScanResult"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class ScanResult(Base):
    __tablename__ = "scan_results"
    __table_args__ = (UniqueConstraint("run_id", "stock_id", name="uq_scan_result_run_stock"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("scan_runs.id", ondelete="CASCADE"), index=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id", ondelete="CASCADE"), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    dg: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gg: Mapped[float | None] = mapped_column(Float, nullable=True)
    rsi: Mapped[float | None] = mapped_column(Float, nullable=True)
    d1: Mapped[float | None] = mapped_column(Float, nullable=True)
    d3: Mapped[float | None] = mapped_column(Float, nullable=True)
    d5: Mapped[float | None] = mapped_column(Float, nullable=True)
    d20: Mapped[float | None] = mapped_column(Float, nullable=True)
    d60: Mapped[float | None] = mapped_column(Float, nullable=True)
    reversal_date: Mapped[str | None] = mapped_column(String(16), nullable=True)
    run: Mapped[ScanRun] = relationship(back_populates="results")
    stock: Mapped[Stock] = relationship(back_populates="results")


class DailyPrice(Base):
    __tablename__ = "daily_prices"
    __table_args__ = (
        UniqueConstraint("stock_id", "trading_date", name="uq_daily_price_stock_date"),
        Index("ix_daily_prices_stock_date", "stock_id", "trading_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id", ondelete="CASCADE"), index=True)
    trading_date: Mapped[date] = mapped_column(Date)
    close: Mapped[float] = mapped_column(Float)
    ma3: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


_ENGINE_OPTIONS: dict[str, object] = {"pool_pre_ping": True}
if not DATABASE_URL.startswith("sqlite"):
    _ENGINE_OPTIONS.update(
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
        pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "10")),
        pool_recycle=300,
    )
ENGINE = create_engine(DATABASE_URL, **_ENGINE_OPTIONS)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False)
_DB_LOCK = threading.Lock()
_DB_READY = False

def ensure_database() -> bool:
    global _DB_READY
    if _DB_READY:
        return True
    with _DB_LOCK:
        if _DB_READY:
            return True
        try:
            Base.metadata.create_all(ENGINE)
            columns = {column["name"] for column in inspect(ENGINE).get_columns("scan_results")}
            if "d60" not in columns:
                with ENGINE.begin() as connection:
                    connection.execute(text("ALTER TABLE scan_results ADD COLUMN d60 FLOAT"))
            with ENGINE.begin() as connection:
                connection.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_scan_results_stock_observed "
                        "ON scan_results (stock_id, observed_at)"
                    )
                )
            _DB_READY = True
        except Exception as exc:
            app.logger.warning("Database initialization failed: %s", exc)
            return False
    return True


def _load_bundled_stocks() -> list[dict]:
    with DATA_FILE.open(encoding="utf-8") as handle:
        return json.load(handle)


def code_to_ticker(code: str, market: str = "CN") -> str:
    """Convert mainland or Hong Kong codes to Yahoo Finance notation."""
    market = str(market).strip().upper()
    if market == "HK":
        return f"{str(int(str(code).strip())).zfill(4)}.HK"
    code = str(code).strip().zfill(6)
    if code == "399001":
        return "399001.SZ"
    if code.startswith("92"):
        return f"{code}.BJ"
    if code.startswith(("5", "6", "9")):
        return f"{code}.SS"
    if code.startswith(("0", "1", "2", "3")):
        return f"{code}.SZ"
    return code


def stock_research_links(code: str, market: str = "CN", ticker: str = "") -> dict[str, str]:
    """Build an exchange-aware TradingView stock-detail link."""
    market = str(market).strip().upper()
    code = str(code).strip()
    ticker = str(ticker).strip().upper()

    if market == "HK":
        hk_code = str(int(code)).zfill(5)
        tradingview_code = str(int(hk_code))
        return {
            "tradingview_url": f"https://www.tradingview.com/symbols/HKEX-{tradingview_code}/?timeframe=1M",
        }

    cn_code = code.zfill(6)
    if ticker.endswith(".BJ") or cn_code.startswith("92"):
        return {
            "tradingview_url": f"https://www.tradingview.com/search/?query={cn_code}",
        }
    is_shanghai = ticker.endswith(".SS") or (
        not ticker.endswith(".SZ") and cn_code.startswith(("5", "6", "9"))
    )
    tradingview_exchange = "SSE" if is_shanghai else "SZSE"
    return {
        "tradingview_url": f"https://www.tradingview.com/symbols/{tradingview_exchange}-{cn_code}/?timeframe=1M",
    }


def _parse_gviz(text: str) -> dict:
    prefix = "google.visualization.Query.setResponse("
    start = text.find(prefix)
    end = text.rfind(");")
    if start < 0 or end < 0:
        raise ValueError("Unexpected Google Sheets response")
    return json.loads(text[start + len(prefix) : end])


def _load_public_sheet() -> list[dict]:
    """Load the source sheet when it is publicly readable."""
    query = urllib.parse.urlencode(
        {"sheet": SOURCE_SHEET_NAME, "tqx": "out:json"}
    )
    url = (
        f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?{query}"
    )
    with urllib.request.urlopen(url, timeout=8) as response:
        payload = _parse_gviz(response.read().decode("utf-8"))

    table = payload.get("table", {})
    rows = []
    for raw_row in table.get("rows", []):
        values = [cell.get("v", "") if cell else "" for cell in raw_row.get("c", [])]
        if len(values) < 4 or str(values[0]).strip() in {"", "選股條件"}:
            continue
        market = str(values[1] or "CN").strip().upper()
        width = 5 if market == "HK" else 6
        code = str(values[2]).strip().zfill(width)
        if not code.isdigit():
            continue
        rows.append(
            {
                "category": str(values[0]).strip(),
                "market": market,
                "code": code,
                "name": str(values[3]).strip(),
                "ticker": code_to_ticker(code, market),
                "as_of": str(values[4]).strip() if len(values) > 4 else "",
                "source": str(values[5]).strip() if len(values) > 5 else "",
            }
        )
    if not rows:
        raise ValueError("No stock rows found in source sheet")
    return rows


def _sync_stocks_to_database(rows: list[dict]) -> list[dict] | None:
    if not ensure_database():
        return None
    with SessionLocal() as session:
        existing = {stock.code: stock for stock in session.scalars(select(Stock)).all()}
        for stock in existing.values():
            stock.active = False
        for row in rows:
            stock = existing.get(row["code"])
            if stock is None:
                stock = Stock(code=row["code"], ticker=row["ticker"], name=row["name"], category=row["category"])
                session.add(stock)
            stock.ticker = row["ticker"]
            stock.name = row["name"]
            stock.category = row["category"]
            stock.market = row.get("market", "CN")
            stock.as_of = row.get("as_of", "")
            stock.source = row.get("source", "")
            stock.active = True
        session.commit()
        active = session.scalars(
            select(Stock).where(Stock.active.is_(True)).order_by(Stock.id)
        ).all()
        return [_stock_to_dict(stock) for stock in active]


def _stock_to_dict(stock: Stock) -> dict:
    row = {
        "category": stock.category,
        "market": stock.market,
        "code": stock.code,
        "name": stock.name,
        "ticker": stock.ticker,
        "as_of": stock.as_of,
        "source": stock.source,
    }
    row.update(stock_research_links(stock.code, stock.market, stock.ticker))
    return row


def _load_database_stocks() -> list[dict]:
    if not ensure_database():
        return []
    with SessionLocal() as session:
        stocks = session.scalars(
            select(Stock).where(Stock.active.is_(True)).order_by(Stock.id)
        ).all()
        return [_stock_to_dict(stock) for stock in stocks]


def sync_stocks_from_source() -> tuple[list[dict], str]:
    """Refresh the database universe. Intended for the scheduled close scan."""
    try:
        rows, source_mode = _load_public_sheet(), "google_sheet"
    except Exception as exc:
        app.logger.warning("Google Sheet unavailable; using bundled snapshot: %s", exc)
        rows, source_mode = _load_bundled_stocks(), "bundled_snapshot"
    database_rows = _sync_stocks_to_database(rows)
    if database_rows is None:
        raise RuntimeError("Database unavailable while syncing stock universe")
    return database_rows, source_mode


def load_stocks() -> tuple[list[dict], str]:
    """Load the universe from SQL; seed it only when the database is empty."""
    database_rows = _load_database_stocks()
    if database_rows:
        return database_rows, "database"
    return sync_stocks_from_source()


def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def calculate_metrics(df: pd.DataFrame) -> dict | None:
    if df is None or df.empty or "Close" not in df or len(df) < 20:
        return None
    close = df["Close"].dropna()
    if len(close) < 20:
        return None

    ma3 = close.rolling(3).mean()
    work = pd.DataFrame({"Close": close, "MA3": ma3}).dropna()
    if len(work) < 5:
        return None

    below = work["Close"] < work["MA3"]
    segments: list[tuple[int, int]] = []
    start = None
    for index, is_below in enumerate(below.tolist()):
        if is_below and start is None:
            start = index
        elif not is_below and start is not None:
            segments.append((start, index - 1))
            start = None
    if start is not None:
        segments.append((start, len(work) - 1))
    if not segments:
        return None

    reversal_dates = [work.iloc[a : b + 1]["Close"].idxmin() for a, b in segments]
    last_reversal = reversal_dates[-1]
    last_price = float(work["Close"].iloc[-1])
    reversal_price = float(work.loc[last_reversal, "Close"])
    latest_date = pd.Timestamp(work.index[-1])
    reversal_date = pd.Timestamp(last_reversal)
    rsi = calculate_rsi(close)

    def gain(days: int) -> float | None:
        if len(close) <= days:
            return None
        old_price = float(close.iloc[-days - 1])
        return round((last_price / old_price - 1) * 100, 2)

    return {
        "price": round(last_price, 2),
        "dg": int((latest_date - reversal_date).days),
        "gg": round((last_price / reversal_price - 1) * 100, 2),
        "rsi": round(float(rsi.iloc[-1]), 1) if pd.notna(rsi.iloc[-1]) else None,
        "d1": gain(1),
        "d3": gain(3),
        "d5": gain(5),
        "d20": gain(20),
        "d60": gain(60),
        "reversal_date": reversal_date.strftime("%Y-%m-%d"),
    }


def _extract_ticker_frame(data: pd.DataFrame, ticker: str, ticker_count: int):
    if data is None or data.empty:
        return None
    if ticker_count == 1 and not isinstance(data.columns, pd.MultiIndex):
        return data
    if not isinstance(data.columns, pd.MultiIndex):
        return None
    level_zero = data.columns.get_level_values(0)
    if ticker not in level_zero:
        return None
    return data[ticker].copy()


def _price_history(frame: pd.DataFrame | None) -> list[dict]:
    if frame is None or frame.empty or "Close" not in frame:
        return []
    close = frame["Close"].dropna()
    ma3 = close.rolling(3).mean()
    return [
        {
            "trading_date": pd.Timestamp(index).date(),
            "close": round(float(value), 4),
            "ma3": None if pd.isna(ma3.loc[index]) else round(float(ma3.loc[index]), 4),
        }
        for index, value in close.items()
    ]


def _download_chunk(tickers: list[str]) -> dict[str, dict]:
    try:
        data = yf.download(
            tickers,
            period="6mo",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
            timeout=20,
        )
    except Exception:
        return {ticker: {"metrics": None, "prices": []} for ticker in tickers}

    result = {}
    for ticker in tickers:
        frame = _extract_ticker_frame(data, ticker, len(tickers))
        result[ticker] = {
            "metrics": calculate_metrics(frame),
            "prices": _price_history(frame),
        }
    return result


def scan_stocks_with_prices(stocks: list[dict]) -> tuple[list[dict], dict[str, list[dict]]]:
    tickers = list(dict.fromkeys(stock["ticker"] for stock in stocks))
    chunks = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    payloads: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(chunks) or 1)) as pool:
        futures = [pool.submit(_download_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            payloads.update(future.result())

    rows = []
    for stock in stocks:
        row = dict(stock)
        row.update(
            payloads.get(stock["ticker"], {}).get("metrics")
            or {
                "price": None,
                "dg": None,
                "gg": None,
                "rsi": None,
                "d1": None,
                "d3": None,
                "d5": None,
                "d20": None,
                "d60": None,
                "reversal_date": None,
            }
        )
        rows.append(row)
    rows.sort(key=lambda row: (row["dg"] is None, row["dg"] or 0, row["code"]))
    histories = {
        ticker: payload.get("prices", []) for ticker, payload in payloads.items()
    }
    return rows, histories


def scan_stocks(stocks: list[dict]) -> list[dict]:
    rows, _ = scan_stocks_with_prices(stocks)
    return rows


def _upsert_daily_prices(
    session, stocks: dict[str, Stock], histories: dict[str, list[dict]]
) -> None:
    now = datetime.now(timezone.utc)
    records = [
        {
            "stock_id": stocks[ticker].id,
            "trading_date": item["trading_date"],
            "close": item["close"],
            "ma3": item["ma3"],
            "updated_at": now,
        }
        for ticker, prices in histories.items()
        if ticker in stocks
        for item in prices
    ]
    if not records:
        return
    dialect_insert = (
        postgresql_insert if ENGINE.dialect.name == "postgresql" else sqlite_insert
    )
    for offset in range(0, len(records), 1000):
        statement = dialect_insert(DailyPrice).values(records[offset : offset + 1000])
        statement = statement.on_conflict_do_update(
            index_elements=[DailyPrice.stock_id, DailyPrice.trading_date],
            set_={
                "close": statement.excluded.close,
                "ma3": statement.excluded.ma3,
                "updated_at": statement.excluded.updated_at,
            },
        )
        session.execute(statement)


def persist_scan(
    rows: list[dict],
    source_mode: str,
    histories: dict[str, list[dict]] | None = None,
) -> int | None:
    if not ensure_database():
        return None
    with SessionLocal() as session:
        stocks = {stock.ticker: stock for stock in session.scalars(select(Stock)).all()}
        run = ScanRun(
            source_mode=source_mode,
            total_count=len(rows),
            available_count=sum(row.get("price") is not None for row in rows),
        )
        session.add(run)
        session.flush()
        observed_at = datetime.now(timezone.utc)
        for row in rows:
            stock = stocks.get(row["ticker"])
            if stock is None:
                continue
            session.add(
                ScanResult(
                    run_id=run.id,
                    stock_id=stock.id,
                    observed_at=observed_at,
                    price=row.get("price"),
                    dg=row.get("dg"),
                    gg=row.get("gg"),
                    rsi=row.get("rsi"),
                    d1=row.get("d1"),
                    d3=row.get("d3"),
                    d5=row.get("d5"),
                    d20=row.get("d20"),
                    d60=row.get("d60"),
                    reversal_date=row.get("reversal_date"),
                )
            )
        _upsert_daily_prices(session, stocks, histories or {})
        run.completed_at = datetime.now(timezone.utc)
        session.commit()
        return run.id


def load_latest_scan() -> dict:
    """Return the latest completed SQL snapshot without contacting market APIs."""
    if not ensure_database():
        raise RuntimeError("Database unavailable")
    with SessionLocal() as session:
        run = session.scalar(
            select(ScanRun)
            .where(ScanRun.completed_at.is_not(None))
            .order_by(ScanRun.completed_at.desc(), ScanRun.id.desc())
            .limit(1)
        )
        condition = (
            and_(ScanResult.stock_id == Stock.id, ScanResult.run_id == run.id)
            if run
            else and_(ScanResult.stock_id == Stock.id, ScanResult.run_id == -1)
        )
        records = session.execute(
            select(Stock, ScanResult)
            .outerjoin(ScanResult, condition)
            .where(Stock.active.is_(True))
            .order_by(Stock.id)
        ).all()
        rows = []
        for stock, result in records:
            row = _stock_to_dict(stock)
            row.update(
                {
                    "price": result.price if result else None,
                    "dg": result.dg if result else None,
                    "gg": result.gg if result else None,
                    "rsi": result.rsi if result else None,
                    "d1": result.d1 if result else None,
                    "d3": result.d3 if result else None,
                    "d5": result.d5 if result else None,
                    "d20": result.d20 if result else None,
                    "d60": result.d60 if result else None,
                    "reversal_date": result.reversal_date if result else None,
                }
            )
            rows.append(row)
        rows.sort(key=lambda row: (row["dg"] is None, row["dg"] or 0, row["code"]))
        return {
            "rows": rows,
            "run_id": run.id if run else None,
            "source": run.source_mode if run else "database_pending",
            "generated_at": run.completed_at.isoformat() if run and run.completed_at else None,
        }


def database_summary() -> dict:
    if not ensure_database():
        return {"connected": False, "engine": "unavailable"}
    with SessionLocal() as session:
        stock_count = session.scalar(select(func.count()).select_from(Stock).where(Stock.active.is_(True))) or 0
        run_count = session.scalar(select(func.count()).select_from(ScanRun)) or 0
        result_count = session.scalar(select(func.count()).select_from(ScanResult)) or 0
        price_count = session.scalar(select(func.count()).select_from(DailyPrice)) or 0
        latest = session.scalar(select(ScanRun).order_by(ScanRun.id.desc()).limit(1))
        return {
            "connected": True,
            "engine": ENGINE.dialect.name,
            "stocks": stock_count,
            "scan_runs": run_count,
            "scan_results": result_count,
            "daily_prices": price_count,
            "latest_run_id": latest.id if latest else None,
            "latest_completed_at": latest.completed_at.isoformat() if latest and latest.completed_at else None,
        }


@app.context_processor
def inject_links():
    return {
        "base_url": APP_BASE_URL,
        "production_url": PRODUCTION_URL,
        "github_repo_url": GITHUB_REPO_URL,
        "google_sheet_url": GOOGLE_SHEET_URL,
    }


@app.get("/")
def index():
    stocks, source_mode = load_stocks()
    categories = list(dict.fromkeys(stock["category"] for stock in stocks))
    as_of = max((stock.get("as_of", "") for stock in stocks), default="")
    return render_template(
        "index.html",
        categories=categories,
        stock_count=len(stocks),
        as_of=as_of,
        source_mode=source_mode,
    )


@app.get("/api/stocks")
def stock_universe():
    stocks, source_mode = load_stocks()
    return jsonify({"stocks": stocks, "count": len(stocks), "source": source_mode})


@app.get("/api/data")
def data():
    try:
        payload = load_latest_scan()
        return jsonify({**payload, "cached": True})
    except Exception as exc:
        app.logger.exception("Could not load latest database scan")
        return jsonify({"error": str(exc)}), 503


@app.get("/api/database-status")
def database_status():
    summary = database_summary()
    return jsonify(summary), 200 if summary["connected"] else 503


@app.get("/api/history/<ticker>")
def price_history(ticker: str):
    ticker = ticker.upper()
    limit = min(max(request.args.get("limit", default=30, type=int), 1), 365)
    if not ensure_database():
        return jsonify({"error": "Database unavailable"}), 503
    with SessionLocal() as session:
        statement = (
            select(ScanResult, ScanRun)
            .join(Stock, ScanResult.stock_id == Stock.id)
            .join(ScanRun, ScanResult.run_id == ScanRun.id)
            .where(Stock.ticker == ticker)
            .order_by(ScanResult.observed_at.desc())
            .limit(limit)
        )
        records = session.execute(statement).all()
        return jsonify(
            {
                "ticker": ticker,
                "history": [
                    {
                        "run_id": result.run_id,
                        "observed_at": result.observed_at.isoformat(),
                        "price": result.price,
                        "dg": result.dg,
                        "gg": result.gg,
                        "rsi": result.rsi,
                        "d1": result.d1,
                        "d3": result.d3,
                        "d5": result.d5,
                        "d20": result.d20,
                        "d60": result.d60,
                        "reversal_date": result.reversal_date,
                        "source": run.source_mode,
                    }
                    for result, run in records
                ],
            }
        )


@app.get("/api/chart/<ticker>")
def chart(ticker: str):
    ticker = ticker.upper()
    if ensure_database():
        with SessionLocal() as session:
            records = session.scalars(
                select(DailyPrice)
                .join(Stock, DailyPrice.stock_id == Stock.id)
                .where(Stock.ticker == ticker)
                .order_by(DailyPrice.trading_date.desc())
                .limit(70)
            ).all()
            if records:
                records.reverse()
                return jsonify(
                    {
                        "ticker": ticker,
                        "dates": [item.trading_date.isoformat() for item in records],
                        "prices": [round(item.close, 3) for item in records],
                        "ma3": [
                            None if item.ma3 is None else round(item.ma3, 3)
                            for item in records
                        ],
                        "source": "database",
                    }
                )
    try:
        frame = yf.Ticker(ticker).history(period="3mo", auto_adjust=False)
        if frame.empty:
            return jsonify({"error": "No Yahoo Finance data found"}), 404
        close = frame["Close"].dropna()
        ma3 = close.rolling(3).mean()
        return jsonify(
            {
                "ticker": ticker,
                "dates": [pd.Timestamp(value).strftime("%Y-%m-%d") for value in close.index],
                "prices": [round(float(value), 3) for value in close],
                "ma3": [None if pd.isna(value) else round(float(value), 3) for value in ma3],
                "source": "yahoo_fallback",
            }
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/health")
def health():
    return jsonify({"status": "ok", "app": "Chinareversals", "database": ensure_database()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
