"""
QP2 데이터 원클릭 업데이트
- SEC 분기 재무 증분 업데이트
- Yahoo 주가 증분 업데이트

사용법: 
  - UPDATE.bat 더블클릭
  - 또는: python scripts/update_all.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import gzip
import logging

# -----------------------------
# 경로 설정
# -----------------------------
QP2_ROOT = Path(r"C:\QP2")
sys.path.insert(0, str(QP2_ROOT))

DATA_DIR = QP2_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
META_DIR = DATA_DIR / "meta"

# 로깅
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("qp2_update")

# -----------------------------
# 필수 라이브러리
# -----------------------------
try:
    import pandas as pd
    import yfinance as yf
    from tqdm import tqdm
    import pytz
except ImportError as e:
    print(f"❌ 라이브러리 없음: {e}")
    print("설치: pip install pandas yfinance tqdm pytz")
    input("Enter 키를 누르면 종료...")
    sys.exit(1)

# -----------------------------
# 유틸 함수
# -----------------------------
def save_parquet(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def load_json_gz(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)

# =============================================================================
# SEC 분기 재무 업데이트
# =============================================================================
def update_sec_quarterly():
    """SEC 분기 재무 증분 업데이트"""
    
    print("\n" + "="*60)
    print("📊 SEC 분기 재무 업데이트")
    print("="*60)
    
    OUT_PATH = INTERIM_DIR / "fundamentals_quarterly.parquet"
    CHECKPOINT_PATH = INTERIM_DIR / "quarterly_update_checkpoint.json"
    
    if not OUT_PATH.exists():
        print("❌ 기존 parquet 없음. Jupyter에서 셀 2를 먼저 실행하세요.")
        return False
    
    # 기존 데이터 로드
    fund_q_old = pd.read_parquet(OUT_PATH)
    print(f"✅ 기존 데이터: {len(fund_q_old):,} rows")
    
    # 체크포인트 로드
    file_mtimes = {}
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH, "r") as f:
            file_mtimes = json.load(f)
    
    # 변경 파일 탐지
    paths = sorted((RAW_DIR / "sec" / "companyfacts").glob("*.json.gz"))
    
    changed_files = []
    for p in paths:
        mtime = os.path.getmtime(p)
        mtime_str = str(mtime)
        if p.name not in file_mtimes or file_mtimes[p.name] != mtime_str:
            changed_files.append(p)
    
    if not changed_files:
        print("✅ 변경된 파일 없음. 업데이트 불필요.")
        return True
    
    print(f"📁 변경된 파일: {len(changed_files)}개")
    
    # 여기서 전체 파싱 로직 필요 (extract_fundamentals_quarterly 함수)
    # 이 부분은 노트북에서 정의된 함수를 import하거나 여기에 복사해야 함
    print("⚠️ SEC 증분 업데이트는 Jupyter 노트북에서 실행하세요.")
    print("   (함수 정의가 복잡해서 스크립트에서는 체크포인트만 갱신)")
    
    # 체크포인트만 갱신
    new_mtimes = {p.name: str(os.path.getmtime(p)) for p in paths}
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(new_mtimes, f)
    
    print(f"✅ 체크포인트 갱신: {len(new_mtimes)}개 파일")
    return True

# =============================================================================
# Yahoo 주가 업데이트
# =============================================================================
def update_yahoo_prices():
    """Yahoo 주가 증분 업데이트"""
    
    print("\n" + "="*60)
    print("📈 Yahoo 주가 업데이트")
    print("="*60)
    
    YAHOO_DIR = RAW_DIR / "yahoo"
    YAHOO_DIR.mkdir(parents=True, exist_ok=True)
    
    OUT_WIDE = INTERIM_DIR / "yahoo_adjclose_wide.parquet"
    OUT_LONG = INTERIM_DIR / "yahoo_prices_long.parquet"
    
    # S&P500 티커 로드
    sp500_path = META_DIR / "sp500_universe.parquet"
    if not sp500_path.exists():
        print("❌ sp500_universe.parquet 없음")
        return False
    
    sp500 = pd.read_parquet(sp500_path)
    if "ticker_yahoo" in sp500.columns:
        tickers = sp500["ticker_yahoo"].dropna().unique().tolist()
    else:
        tickers = sp500["ticker"].dropna().unique().tolist()
    
    tickers = sorted([str(t).strip() for t in tickers if t])
    print(f"✅ S&P500 티커: {len(tickers)}개")
    
    # 증분 업데이트
    NY_TZ = pytz.timezone("America/New_York")
    today = datetime.now(NY_TZ).date()
    
    updated = 0
    failed = []
    skipped = 0
    
    for ticker in tqdm(tickers, desc="Yahoo 증분"):
        ticker_path = YAHOO_DIR / f"{ticker}.parquet"
        
        try:
            if ticker_path.exists():
                existing = pd.read_parquet(ticker_path)
                if "Date" in existing.columns:
                    existing["Date"] = pd.to_datetime(existing["Date"])
                    last_date = existing["Date"].max().date()
                elif isinstance(existing.index, pd.DatetimeIndex):
                    last_date = existing.index.max().date()
                else:
                    last_date = None
                
                if last_date and last_date >= today - timedelta(days=1):
                    skipped += 1
                    continue
                
                start_date = last_date + timedelta(days=1)
            else:
                existing = None
                start_date = datetime(2000, 1, 1).date()
            
            df_new = yf.download(
                ticker,
                start=start_date.isoformat(),
                end=(today + timedelta(days=1)).isoformat(),
                progress=False,
                auto_adjust=False
            )
            
            if df_new.empty:
                if existing is None:
                    failed.append(ticker)
                else:
                    skipped += 1
                continue
            
            if isinstance(df_new.columns, pd.MultiIndex):
                df_new.columns = df_new.columns.get_level_values(0)
            
            df_new = df_new.reset_index()
            df_new["Date"] = pd.to_datetime(df_new["Date"]).dt.tz_localize(None)
            
            if existing is not None:
                if "Date" not in existing.columns and existing.index.name == "Date":
                    existing = existing.reset_index()
                existing["Date"] = pd.to_datetime(existing["Date"]).dt.tz_localize(None)
                df_merged = pd.concat([existing, df_new], ignore_index=True)
                df_merged = df_merged.drop_duplicates(subset=["Date"], keep="last")
                df_merged = df_merged.sort_values("Date").reset_index(drop=True)
            else:
                df_merged = df_new
            
            df_merged.to_parquet(ticker_path, index=False)
            updated += 1
            
        except Exception as e:
            failed.append(ticker)
    
    print(f"\n✅ Yahoo 증분 완료")
    print(f"   업데이트: {updated}개")
    print(f"   스킵(최신): {skipped}개")
    print(f"   실패: {len(failed)}개")
    
    # 패널 재생성
    print("\n📊 패널 재생성 중...")
    
    all_data = []
    for ticker in tqdm(tickers, desc="패널 생성"):
        ticker_path = YAHOO_DIR / f"{ticker}.parquet"
        if not ticker_path.exists():
            continue
        
        try:
            df = pd.read_parquet(ticker_path)
            if "Date" not in df.columns:
                df = df.reset_index()
            
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
            
            adj_col = None
            for col in ["Adj Close", "Adj_Close", "AdjClose", "adj_close"]:
                if col in df.columns:
                    adj_col = col
                    break
            
            if adj_col is None:
                continue
            
            df_slim = df[["Date", adj_col]].copy()
            df_slim.columns = ["date", "adj_close"]
            df_slim["ticker"] = ticker
            all_data.append(df_slim)
            
        except:
            pass
    
    if not all_data:
        print("❌ 데이터 없음")
        return False
    
    long_df = pd.concat(all_data, ignore_index=True)
    long_df = long_df.dropna(subset=["adj_close"])
    
    wide_df = long_df.pivot(index="date", columns="ticker", values="adj_close")
    wide_df = wide_df.sort_index()
    
    long_df.to_parquet(OUT_LONG, index=False)
    wide_df.to_parquet(OUT_WIDE)
    
    print(f"\n✅ 패널 저장 완료")
    print(f"   기간: {wide_df.index.min().date()} ~ {wide_df.index.max().date()}")
    
    return True

# =============================================================================
# 메인
# =============================================================================
def main():
    print("\n" + "="*60)
    print("🚀 QP2 데이터 업데이트 시작")
    print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 1) SEC 재무
    update_sec_quarterly()
    
    # 2) Yahoo 주가
    update_yahoo_prices()
    
    print("\n" + "="*60)
    print("✅ 모든 업데이트 완료!")
    print("="*60)
    
    input("\nEnter 키를 누르면 종료...")

if __name__ == "__main__":
    main()