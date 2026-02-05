# =============================================================================
# C:\QP2\scripts\update_macro.py
# =============================================================================
# 목적: FRED 거시변수 + 레짐 분류 자동 업데이트
# 실행: python update_macro.py 또는 UPDATE_MACRO.bat
# 수정: pandas_datareader → fredapi (Python 3.14 호환)
# =============================================================================

import os
import sys
from pathlib import Path
from datetime import datetime

# 경로 설정
QP2_ROOT = Path("C:/QP2")
sys.path.insert(0, str(QP2_ROOT))

os.chdir(QP2_ROOT)

from dotenv import load_dotenv
load_dotenv(QP2_ROOT / ".env")

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# -----------------------------------------------------------------------------
# 경로
# -----------------------------------------------------------------------------
INTERIM_DIR = QP2_ROOT / "data" / "interim"
FRED_API_KEY = os.getenv("FRED_API_KEY", "8efc9e4ed1c9c3433ba70a995ede776c")

print("="*60)
print(f"MACRO 업데이트 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

# -----------------------------------------------------------------------------
# 1. FRED 데이터 수집 (fredapi 사용)
# -----------------------------------------------------------------------------
print("\n[1/3] FRED 거시지표 수집 중...")

from fredapi import Fred
fred = Fred(api_key=FRED_API_KEY)

FRED_SERIES = {
    "UNRATE": "실업률",
    "T10Y2Y": "장단기금리차 (10Y-2Y)",
    "T10Y3M": "장단기금리차 (10Y-3M)", 
    "BAMLH0A0HYM2": "하이일드 스프레드",
    "VIXCLS": "VIX",
    "FEDFUNDS": "기준금리",
    "CPIAUCSL": "CPI",
}

try:
    macro_data = {}
    for series_id, name in FRED_SERIES.items():
        try:
            s = fred.get_series(series_id, observation_start="2000-01-01")
            macro_data[series_id] = s
            print(f"  ✅ {series_id} ({name}): {len(s)} rows")
        except Exception as e:
            print(f"  ❌ {series_id} 실패: {e}")
    
    macro_df = pd.DataFrame(macro_data)
    macro_df.index = pd.to_datetime(macro_df.index)
    
    # 월말 리샘플링
    macro_m = macro_df.resample("ME").last()
    macro_m.to_parquet(INTERIM_DIR / "macro_indicators.parquet")
    print(f"  저장: macro_indicators.parquet ({len(macro_m)} months)")

except Exception as e:
    print(f"  ❌ FRED 수집 실패: {e}")
    macro_m = pd.read_parquet(INTERIM_DIR / "macro_indicators.parquet")
    print(f"  기존 파일 사용: {len(macro_m)} months")

# -----------------------------------------------------------------------------
# 2. S&P500 기반 레짐 분류
# -----------------------------------------------------------------------------
print("\n[2/3] S&P500 레짐 분류 중...")

try:
    # Yahoo에서 S&P500 수집
    import yfinance as yf
    
    sp500 = yf.download("^GSPC", start="2000-01-01", progress=False)
    sp500_m = sp500["Close"].resample("ME").last()
    
    # 수익률 계산
    ret_1m = sp500_m.pct_change()
    ret_6m = sp500_m.pct_change(6)
    ret_12m = sp500_m.pct_change(12)
    
    # 변동성 (12개월 롤링)
    vol_12m = ret_1m.rolling(12).std() * np.sqrt(12)
    
    # 레짐 분류 함수
    def classify_regime_v2(row):
        r1, r6, r12, vol = row["ret_1m"], row["ret_6m"], row["ret_12m"], row["vol_12m"]
        
        if pd.isna(r1) or pd.isna(r6) or pd.isna(r12) or pd.isna(vol):
            return "0_Neutral"
        
        # Crash: 1개월 -10% 이하
        if r1 < -0.10:
            return "1_Crash"
        
        # Recovery_Early: 6개월 +20% 이상, 12개월은 아직 마이너스
        if r6 > 0.20 and r12 < 0:
            return "2_Recovery_Early"
        
        # Contraction: 6개월, 12개월 둘 다 마이너스
        if r6 < 0 and r12 < 0:
            return "3_Contraction"
        
        # Recovery_Late: 6개월, 12개월 둘 다 플러스, 변동성 높음
        if r6 > 0 and r12 > 0 and vol > 0.15:
            return "4_Recovery_Late"
        
        # Expansion: 6개월, 12개월 둘 다 플러스, 변동성 낮음
        if r6 > 0 and r12 > 0 and vol <= 0.15:
            return "5_Expansion"
        
        # Peak: 12개월 +30% 이상
        if r12 > 0.30:
            return "6_Peak"
        
        return "0_Neutral"
    
    market_df = pd.DataFrame({
        "ret_1m": ret_1m,
        "ret_6m": ret_6m,
        "ret_12m": ret_12m,
        "vol_12m": vol_12m
    })
    
    market_df["regime_v2"] = market_df.apply(classify_regime_v2, axis=1)
    market_df.to_parquet(INTERIM_DIR / "market_regime_indicators.parquet")
    print(f"  저장: market_regime_indicators.parquet ({len(market_df)} months)")
    print(f"  최신 레짐: {market_df['regime_v2'].iloc[-1]}")

except Exception as e:
    print(f"  ❌ S&P500 레짐 분류 실패: {e}")

# -----------------------------------------------------------------------------
# 3. 통합 레짐 파일 생성
# -----------------------------------------------------------------------------
print("\n[3/3] 통합 레짐 파일 생성 중...")

try:
    # 기존 파일 로드
    market_regime = pd.read_parquet(INTERIM_DIR / "market_regime_indicators.parquet")
    
    # macro 기반 레짐 (간단 버전)
    macro_m = pd.read_parquet(INTERIM_DIR / "macro_indicators.parquet")
    
    def classify_macro_regime(row):
        vix = row.get("VIXCLS", np.nan)
        spread = row.get("T10Y2Y", np.nan)
        hy = row.get("BAMLH0A0HYM2", np.nan)
        
        if pd.isna(vix) or pd.isna(spread):
            return "0_Neutral"
        
        # 위기: VIX 30 이상 or 하이일드 스프레드 6% 이상
        if vix > 30 or (not pd.isna(hy) and hy > 6):
            return "3_Contraction"
        
        # 금리 역전: 장단기 금리차 마이너스
        if spread < 0:
            return "6_Peak"
        
        # 안정: VIX 20 이하
        if vix < 20:
            return "5_Expansion"
        
        return "0_Neutral"
    
    macro_m["regime_macro"] = macro_m.apply(classify_macro_regime, axis=1)
    
    # 통합
    combined = market_regime[["regime_v2"]].copy()
    combined = combined.join(macro_m[["regime_macro"]], how="outer")
    combined = combined.ffill()
    
    # regime_combined: 둘이 일치하면 확정, 불일치면 market 우선
    def combine_regime(row):
        r_market = row["regime_v2"]
        r_macro = row["regime_macro"]
        
        if pd.isna(r_market):
            return r_macro
        if pd.isna(r_macro):
            return r_market
        if r_market == r_macro:
            return r_market
        return r_market  # 불일치시 market 우선
    
    combined["regime_combined"] = combined.apply(combine_regime, axis=1)
    
    combined.to_parquet(INTERIM_DIR / "regime_indicators_combined.parquet")
    print(f"  저장: regime_indicators_combined.parquet ({len(combined)} months)")
    
    # 최근 레짐 출력
    latest = combined.iloc[-1]
    print(f"\n  📊 최신 레짐 상태:")
    print(f"     - regime_v2 (S&P500): {latest['regime_v2']}")
    print(f"     - regime_macro (거시): {latest['regime_macro']}")
    print(f"     - regime_combined: {latest['regime_combined']}")

except Exception as e:
    print(f"  ❌ 통합 파일 생성 실패: {e}")

# -----------------------------------------------------------------------------
# 완료
# -----------------------------------------------------------------------------
print("\n" + "="*60)
print(f"MACRO 업데이트 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

input("\n아무 키나 누르면 종료...")