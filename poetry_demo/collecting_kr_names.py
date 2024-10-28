from pykrx import stock
import pandas as pd
from datetime import datetime
import os
import logging
from typing import Dict, List


def setup_logging():
    """
    Databricks 환경에 맞는 로깅 설정
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def get_market_stocks(market: str, logger: logging.Logger) -> List[Dict]:
    """
    지정된 시장의 종목 정보를 수집
    """
    logger.info(f"{market} 시장 종목 수집 시작")
    tickers = stock.get_market_ticker_list(market=market)
    logger.info(f"{market} 시장 총 {len(tickers)}개 종목 발견")

    stock_info = []
    for idx, ticker in enumerate(tickers, 1):
        try:
            name = stock.get_market_ticker_name(ticker)
            stock_info.append({
                'ticker': ticker,
                'name': name,
                'market': market
            })

            # 진행상황 로깅 (10% 단위)
            if idx % (len(tickers) // 10) == 0:
                progress = (idx / len(tickers)) * 100
                logger.info(f"{market} 시장 {progress:.1f}% 완료")

        except Exception as e:
            logger.error(f"종목 {ticker} 처리 중 오류 발생: {str(e)}")
            continue

    return stock_info


def collect_stock_names():
    """
    한국거래소의 모든 종목 정보를 수집하는 함수
    """
    logger = setup_logging()
    try:
        logger.info("종목명 수집 프로세스 시작")

        # 오늘 날짜 가져오기
        today = datetime.today().strftime("%Y%m%d")
        logger.info(f"수집 날짜: {today}")

        # KOSPI 종목 수집
        logger.info("KOSPI 종목 수집 시작")
        kospi_names = get_market_stocks("KOSPI", logger)
        logger.info(f"KOSPI 종목 {len(kospi_names)}개 수집 완료")

        # KOSDAQ 종목 수집
        logger.info("KOSDAQ 종목 수집 시작")
        kosdaq_names = get_market_stocks("KOSDAQ", logger)
        logger.info(f"KOSDAQ 종목 {len(kosdaq_names)}개 수집 완료")

        # DataFrame으로 변환
        df = pd.DataFrame(kospi_names + kosdaq_names)
        logger.info(f"전체 {len(df)}개 종목 데이터프레임 생성 완료")

        # 파일 저장 경로 설정
        current_path = os.getcwd()
        print(current_path)
        file_path = f'/{current_path}/stock_names_{today}.csv'

        # CSV 파일로 저장
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        logger.info(f"데이터 저장 완료: {file_path}")

        # 데이터 검증
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            logger.info(f"저장된 파일 크기: {file_size / 1024:.2f}KB")

        return "데이터 수집 완료"

    except Exception as e:
        logger.error(f"프로세스 실행 중 오류 발생: {str(e)}")
        raise


if __name__ == "__main__":
    collect_stock_names()