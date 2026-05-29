# Weather Batch ETL Pipeline

## 프로젝트 소개

OpenWeather API 데이터를 수집하여 S3 raw layer에 저장하고,
PostgreSQL 적재 및 dbt transformation을 통해 분석용 mart를 생성하는
Batch ETL Pipeline 프로젝트입니다.

또한 Airflow 기반 orchestration과 GitHub Actions CI를 적용하여
데이터 파이프라인 자동화 및 데이터 품질 검증을 수행했습니다.

---

# Architecture

OpenWeather API
→ Airflow DAG
→ S3 Raw Layer
→ PostgreSQL
→ dbt Transform
→ Weather Mart
→ GitHub Actions CI

---

# 기술 스택

| 영역                     | 기술             |
| ---------------------- | -------------- |
| Workflow Orchestration | Apache Airflow |
| Storage                | AWS S3         |
| Database               | PostgreSQL     |
| Transform              | dbt            |
| CI/CD                  | GitHub Actions |
| Container              | Docker         |

---

# ETL Flow

## 1. API → S3

* OpenWeather API 호출
* JSON 데이터 수집
* CSV 변환
* S3 raw layer 저장

## 2. S3 → PostgreSQL

* S3 CSV 다운로드
* weather_raw 테이블 적재

## 3. PostgreSQL → dbt

* staging 모델 생성
* mart 집계 테이블 생성
* dbt test 수행

---

# dbt Model Structure

weather_raw
↓
stg_weather
↓
mart_weather_daily

---

# CI/CD

GitHub Actions 기반 CI를 구축하여:

* dbt run 자동 실행
* dbt test 자동 검증
* Pull Request 및 Push 시 자동 실행

데이터 품질 및 파이프라인 안정성을 확보했습니다.

---

# 실행 방법

```bash
docker compose up
```

Airflow UI:

http://localhost:8080

---

# 주요 개선 포인트

* 날짜 기반 S3 partition 적용
* Raw Layer 기반 재처리 가능 구조 설계
* staging / mart 계층 분리
* dbt test 기반 데이터 품질 검증
* GitHub Actions 기반 CI 자동화


               +-------------------+
               | OpenWeather API   |
               +-------------------+
                         |
                         v
               +-------------------+
               | Apache Airflow    |
               | (Batch DAG)       |
               +-------------------+
                         |
                         v
               +-------------------+
               | AWS S3 Raw Layer  |
               +-------------------+
                         |
                         v
               +-------------------+
               | PostgreSQL (RDS)  |
               +-------------------+
                         |
                         v
               +-------------------+
               | dbt Transform     |
               | staging / mart    |
               +-------------------+
                         |
                         v
               +-------------------+
               | Weather Mart      |
               +-------------------+

       GitHub Actions
              |
              v
      dbt run / dbt test
