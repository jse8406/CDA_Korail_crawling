import sqlite3
import pandas as pd
from datetime import datetime
import calendar

# 데이터베이스 경로
db_path = 'db/priceDB.db'
csv_path = './html/price_mp_2025_04_to_now.csv'

# SQLite 연결
conn = sqlite3.connect(db_path)

# 현재 날짜
now = datetime.now()
current_year = now.year
current_month = now.month

# 시작 연도와 월
start_year = 2025
start_month = 4

# 데이터를 저장할 리스트
data_frames = []

# 시작 연도부터 현재 연도까지 반복
for year in range(start_year, current_year + 1):
    # 시작 월 설정: 시작 연도면 4월부터, 아니면 1월부터
    month_start = start_month if year == start_year else 1
    # 종료 월 설정: 현재 연도면 현재 월까지, 아니면 12월까지
    month_end = current_month if year == current_year else 12

    for month in range(month_start, month_end + 1):
        # 해당 월의 마지막 날 계산
        last_day = calendar.monthrange(year, month)[1]
        start_date = f"{year}.{month:02d}.01"
        end_date = f"{year}.{month:02d}.{last_day}"

        # 쿼리 실행
        query = f"""
        SELECT ann_num, date, base_price, predict_price, bid_price
        FROM price_set
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date DESC;
        """
        df = pd.read_sql(query, conn)

        if not df.empty:
            data_frames.append(df)

# 모든 데이터를 하나의 DataFrame으로 합치기
if data_frames:
    all_data = pd.concat(data_frames, ignore_index=True)
    # 날짜 내림차순으로 정렬
    all_data = all_data.sort_values(by='date', ascending=False).reset_index(drop=True)
    # CSV 파일로 저장 (컬럼명 변경)
    all_data.to_csv(csv_path, index=False, encoding='utf-8-sig', header=['공고번호', '날짜', '기초가', '예가', '투찰가'])
    print(f"데이터가 {csv_path}에 저장되었습니다.")
else:
    print("해당 기간에 데이터가 없습니다.")

# 연결 종료
conn.close()