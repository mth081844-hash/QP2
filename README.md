# Regime-Switching Multi-Factor Portfolio Model

S&P 500 503종목 대상, 레짐(Bull/Bear/Neutral)별 팩터 조합을 통한 월간 리밸런싱 전략.

**성과 (2013.06 – 2026.01):** CAGR 16.9%, Sharpe 0.96, MaxDD -16.9% | vs SPY +5.2%p (t=1.47)

## 구조

```
QP2/
├── notebooks/                         # 전체 연구 노트북
│   ├── 00_DataLoader_First.ipynb      # Yahoo, SEC EDGAR 초기 수집
│   ├── 01_DataLoader_Macro.ipynb      # 거시변수 + 레짐 판단
│   ├── 01_DataLoader_Quarterly.ipynb  # 분기 재무 + 주가 증분 업데이트
│   ├── 01_DataLoader_Regime.ipynb     # 레짐 분류 (regime_v4)
│   ├── 02_A.ipynb                     # A-3 Value × Catalyst
│   ├── 02_D.ipynb                     # D-1, D-3 모멘텀
│   ├── 02_H.ipynb                     # H 섹터 모멘텀
│   ├── 02_F.ipynb                     # F-1 Piotroski F-Score
│   ├── 02_E&P5.ipynb                  # E-5 저변동, P-5 저베타
│   ├── 02_P-7.ipynb                   # P-7 자사주매입 (NSI)
│   ├── 02_T-1.ipynb                   # T-1 섹터 리더 서지
│   ├── 02_G-1.ipynb                   # G-1 급락반전
│   ├── 05_*.ipynb                     # 개별 팩터 레짐별 재검증
│   ├── 06_TheForge.ipynb              # 멀티팩터 통합 + 가중치 최적화 + WF/MC 검증
│   └── 07_Test_*.ipynb                # 추가 실험 (생존편향 등)
├── outputs/                           # 차트, 백테스트 결과
├── scripts/                           # 배치 스크립트 보조
├── src/                               # 유틸 함수
├── UPDATE*.bat                        # 데이터 증분 업데이트 배치
└── requirements.txt
```

**참고:** `data/` 폴더(Yahoo 주가, SEC 재무, 거시변수 등)는 용량 문제로 repo에 포함되어 있지 않습니다. `00_DataLoader_First.ipynb`부터 순서대로 실행하면 재구축 가능합니다.

## 노트북 흐름

```
00~01 데이터 수집 → 02 팩터 개별 검증 → 05 레짐별 재검증 → 06 통합 모델 (TheForge)
```

## 데이터 소스

| 소스 | 용도 |
|------|------|
| Yahoo Finance | S&P 500 일봉 주가 |
| SEC EDGAR | 연간/분기 재무제표 |
| FRED | VIX, 신용스프레드, EPU 등 거시변수 |
| Finnhub | 단기 실적 서프라이즈 |

## 레짐 체계

| 레짐 | 조건 | 기간 |
|------|------|------|
| Bull | MA 위 + MOM > 0 | 116개월 |
| Bear | MA 아래 + (MOM < 0 OR DD < -10%) | 23개월 |
| Neutral | MA 위 + MOM ≤ 0 | 13개월 |

## 최종 팩터 배치

| 레짐 | 핵심 팩터 | 보조 장치 |
|------|----------|----------|
| Bull | G-1 급락반전(1.8), P-7 자사주(0.3) | T-1 이벤트, G-1b 감점 |
| Bear | E-5 저변동(2.0), A-3 가치×촉매(1.0), H(0.7), D-1(0.7), P-5(0.7) | F-1 필터, G-1b 감점 |
| Neutral | H 섹터(0.5), D-3 변동성조정(1.1) | G-1b 감점 |
