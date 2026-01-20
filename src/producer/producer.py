"""
데이터 수집기
구매 이력 데이터를 사용하여 데이터를 수집하고 kafka로 전송
"""
import pandas as pd
import json
import os
import sys
import time
from datetime import datetime
from kafka import KafkaProducer
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CSV_FILE_PATH
from utils.log_utils import setup_logger

# 다른 모듈 import를 위한 상위디렉토리 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 로깅 설정
logger = setup_logger("producer", "logs/producer.log")

# producer 생성
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"kafka producer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error("Kafka producer 연결 실패: {e}")
        return None
    
def collect_data(producer):
    if not producer:
        logger.info("producer가 없습니다.")
        return
    logger.info("데이터 수집을 시작합니다.")

    # 성능 메트릭
    start_time = time.time()
    message_count = 0
    error_count = 0

    # CSV 파일 읽기
    df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")
    total_records = len(df)

    logger.info(f"총 {total_records}개의 레코드를 전송합니다.")

    # Kafka 메시지 전송
    for idx, row in enumerate(df.iterrows()):
        _, row = row
        message = {
            "invoice_no": str(row["InvoiceNo"]),
            "stock_code": str(row["StockCode"]),
            "description": str(row["Description"]),
            "quantity": int(row["Quantity"]),
            "invoice_date": str(row["InvoiceDate"]),
            "unit_price": float(row["UnitPrice"]),
            "customer_id": str(row["CustomerID"]) if "CustomerID" in row else None,
            "country": str(row["Country"])
        }

        try:
            producer.send(KAFKA_TOPIC, value=message)
            message_count += 1

            # 진행 상황 로깅 (10%마다)
            if (idx + 1) % (total_records // 10) == 0:
                progress = ((idx + 1) / total_records) * 100
                logger.info(f"진행률: {progress:.1f}% ({idx + 1}/{total_records})")

        except Exception as e:
            logger.error(f"메시지 전송 실패: {e}")
            error_count += 1

    producer.flush()

    # 성능 메트릭 계산
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = message_count / elapsed_time if elapsed_time > 0 else 0

    logger.info("=" * 50)
    logger.info("모든 메시지 전송 완료")
    logger.info(f"총 메시지 수: {message_count}")
    logger.info(f"실패 메시지 수: {error_count}")
    logger.info(f"소요 시간: {elapsed_time:.2f}초")
    logger.info(f"처리량(Throughput): {throughput:.2f} msg/sec")
    logger.info("=" * 50)

    return {
        "total_messages": message_count,
        "failed_messages": error_count,
        "elapsed_time": elapsed_time,
        "throughput": throughput
    }



def main():
    logger.info("데이터 수집 시작")
    producer = create_producer()

    try:
        metrics = collect_data(producer)
        return metrics
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다.")
    except Exception as e:
        logger.error(f"오류 발생: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("kafka producer 연결 종료")

if __name__ == "__main__":
    main()
