# =============================================================================
# C:\QP2\scripts\update_regime_v4.py
# =============================================================================
# 목적: regime_v4 (3레짐 + 거시필터) 증분 업데이트
# 실행: python update_regime_v4.py 또는 UPDATE_REGIME_V4.bat
# 의존: macro_indicators.parquet (update_macro.py에서 생성)
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
REGIME_PATH = INTERIM_DIR / "regime_v4.parquet"

# -----------------------------------------------------------------------------
# 파라미터 (01_DataLoader_Regime.ipynb에서 확정)
# -----------------------------------------------------------------------------
MA_WINDOW = 6       # 6개월 SMA
MOM_WINDOW = 3      # 3개월 모멘텀
DD_THRESHOLD = -0.10 # DD 임계값 (보험용, 실질 영향 없음)

print("=" * 60)
print(f"REGIME v4 업데이트 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)
print(f"  파라미터: MA={MA_WINDOW}, MOM={MOM_WINDOW}, DD={DD_THRESHOLD}")

# -----------------------------------------------------------------------------
# 1. EW Price 생성 (S&P500 전 종목 동일가중)
# -----------------------------------------------------------------------------
print("\n[1/4] EW Price 계산 중...")

try:
    px_wide = pd.read_parquet(INTERIM_DIR / "yahoo_adjclose_wide.parquet")
    if "date" in px_wide.columns:
        px_wide = px_wide.set_index("date")
    px_wide.index = pd.to_datetime(px_wide.index)

    # 일간 EW 수익률 → 누적 가격
    ew_ret_1d = px_wide.pct_change().mean(axis=1)
    ew_price = (1 + ew_ret_1d).cumprod() * 1000

    # 월말 리샘플
    ew_monthly = ew_price.resample("ME").last().to_frame("ew_price")

    print(f"  EW Price: {len(ew_monthly)} months")
    print(f"  기간: {ew_monthly.index.min().strftime('%Y-%m')} ~ {ew_monthly.index.max().strftime('%Y-%m')}")

except Exception as e:
    print(f"  ❌ EW Price 계산 실패: {e}")
    sys.exit(1)

# -----------------------------------------------------------------------------
# 2. 3-Regime 분류
# -----------------------------------------------------------------------------
print("\n[2/4] 3-Regime 분류 중...")

price = ew_monthly["ew_price"]

df = pd.DataFrame(index=price.index)
df["price"] = price

# MA + 모멘텀
df["ma"] = df["price"].rolling(MA_WINDOW, min_periods=MA_WINDOW).mean()
df["price_vs_ma"] = df["price"] / df["ma"] - 1
df["mom"] = df["price"].pct_change(MOM_WINDOW)

# Drawdown
df["drawdown"] = df["price"] / df["price"].expanding().max() - 1

# 3개 레짐
above_ma = df["price_vs_ma"] > 0
mom_pos = df["mom"] > 0

df["regime"] = "Neutral"  # MA 위 + mom <= 0
df.loc[above_ma & mom_pos, "regime"] = "Bull"
df.loc[~above_ma, "regime"] = "Bear"

# Bear 내부 세분화 (보조 변수)
df["bear_phase"] = ""
bear_mask = df["regime"] == "Bear"
df.loc[bear_mask & mom_pos, "bear_phase"] = "recovering"
df.loc[bear_mask & ~mom_pos, "bear_phase"] = "declining"

df = df.dropna(subset=["ma"])

print(f"  분류 완료: {len(df)} months")
print(f"  레짐 분포: {df['regime'].value_counts().to_dict()}")

# -----------------------------------------------------------------------------
# 3. 거시 필터 Merge
# -----------------------------------------------------------------------------
print("\n[3/4] 거시 필터 적용 중...")

try:
    macro = pd.read_parquet(INTERIM_DIR / "macro_indicators.parquet")
    macro.index = pd.to_datetime(macro.index)
    macro_m = macro.resample("ME").last()

    # 거시 시그널 (Bear 방향 = True)
    df["baa_rising"] = macro_m["BAMLH0A0HYM2"].diff(3).reindex(df.index) > 0
    df["unrate_rising"] = macro_m["UNRATE"].diff(3).reindex(df.index) > 0
    df["yield_curve_flat"] = macro_m["T10Y2Y"].reindex(df.index) < 0.5
    df["vix_high"] = macro_m["VIXCLS"].reindex(df.index) > 20
    df["t10y3m_inv"] = macro_m["T10Y3M"].reindex(df.index) < 0

    # 거시 필터 카운트
    filter_cols = ["baa_rising", "unrate_rising", "yield_curve_flat", "vix_high", "t10y3m_inv"]
    df["macro_bear_count"] = df[filter_cols].sum(axis=1)

    # 거시 확신도
    df["macro_confirm"] = "none"
    df.loc[df["macro_bear_count"] >= 1, "macro_confirm"] = "weak"
    df.loc[df["macro_bear_count"] >= 3, "macro_confirm"] = "moderate"
    df.loc[df["macro_bear_count"] >= 4, "macro_confirm"] = "strong"

    print(f"  거시 필터 적용 완료")
    print(f"  거시 확신도: {df['macro_confirm'].value_counts().to_dict()}")

except Exception as e:
    print(f"  ⚠ 거시 필터 실패 (레짐만 저장): {e}")
    df["macro_bear_count"] = np.nan
    df["macro_confirm"] = "none"

# -----------------------------------------------------------------------------
# 4. 저장
# -----------------------------------------------------------------------------
print("\n[4/4] 저장 중...")

save_cols = [
    "price", "ma", "price_vs_ma", "mom", "drawdown",
    "regime", "bear_phase",
    "macro_bear_count", "macro_confirm",
]
out = df[save_cols].copy()

# 기존 파일 백업
if REGIME_PATH.exists():
    old = pd.read_parquet(REGIME_PATH)
    backup_name = f"regime_v4_backup_{datetime.now().strftime('%Y%m%d')}.parquet"
    old.to_parquet(INTERIM_DIR / backup_name)
    print(f"  백업: {backup_name}")

out.to_parquet(REGIME_PATH, engine="pyarrow")
print(f"  저장: regime_v4.parquet ({len(out)} months)")

# -----------------------------------------------------------------------------
# 최신 상태 출력
# -----------------------------------------------------------------------------
latest = out.iloc[-1]
prev = out.iloc[-2] if len(out) > 1 else None

print("\n" + "=" * 60)
print("📊 최신 레짐 상태")
print("=" * 60)
print(f"  날짜:        {out.index[-1].strftime('%Y-%m')}")
print(f"  레짐:        {latest['regime']}")
if latest["regime"] == "Bear":
    print(f"  Bear 단계:   {latest['bear_phase']}")
print(f"  Price vs MA: {latest['price_vs_ma']:+.1%}")
print(f"  모멘텀(3M):  {latest['mom']:+.1%}")
print(f"  Drawdown:    {latest['drawdown']:+.1%}")
print(f"  거시 Bear:   {int(latest['macro_bear_count'])}개 ON ({latest['macro_confirm']})")

if prev is not None and prev["regime"] != latest["regime"]:
    print(f"\n  ⚡ 레짐 전환 감지: {prev['regime']} → {latest['regime']}")

# 거시 시그널 상세
print(f"\n  거시 시그널 상세:")
signal_names = {
    "baa_rising": "BAA 스프레드 상승(3M)",
    "unrate_rising": "실업률 상승(3M)",
    "yield_curve_flat": "장단기차 <0.5%",
    "vix_high": "VIX >20",
    "t10y3m_inv": "10Y-3M 역전",
}
for col, name in signal_names.items():
    if col in df.columns:
        val = latest.get(col, False)
        status = "🔴 ON" if val else "🟢 OFF"
        print(f"    {name:<22s}: {status}")

# -----------------------------------------------------------------------------
# 완료
# -----------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"REGIME v4 업데이트 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

input("\n아무 키나 누르면 종료...")
