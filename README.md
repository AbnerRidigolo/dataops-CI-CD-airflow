# dataops-pipeline

[![CI](https://github.com/YOUR_USER/dataops-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USER/dataops-pipeline/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/YOUR_USER/dataops-pipeline/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USER/dataops-pipeline)

Pipeline DataOps completo para o dataset **Brazilian E-Commerce (Olist)** do Kaggle, com validação de dados em todas as camadas, orquestração, CI/CD e observabilidade.

---

## Arquitetura

```
                       ┌──────────────────────────────────────────────────────┐
                       │                  GitHub Actions                     │
                       │         ci.yml (lint+test)  deploy.yml (S3 sync)    │
                       └──────────────┬──────────────────────┬───────────────┘
                                      │                      │
  ┌────────┐   CSV    ┌───────┐  Delta  ┌───────┐  Delta  ┌───────┐  Delta  ┌──────────┐
  │ Kaggle │ ──────▶  │  RAW  │ ──────▶ │BRONZE │ ──────▶ │SILVER │ ──────▶ │  GOLD    │
  │ Olist  │          │(MinIO)│         │(Delta)│         │(Delta)│         │(Delta)   │
  └────────┘          └───┬───┘         └───┬───┘         └───┬───┘         └────┬─────┘
                          │                 │                 │                  │
                          │      ┌──────────┴─────────────────┴──────────────────┘
                          │      │          Great Expectations
                          │      │   bronze_suite  silver_suite  gold_suite
                          │      └──────────┬─────────────────────────────────┐
                          │                 │                                 │
                    ┌─────┴─────┐    ┌──────┴──────┐                  ┌──────┴──────┐
                    │  Airflow  │    │  Data Docs  │                  │    Slack    │
                    │ Scheduler │    │   (MinIO)   │                  │   Alerts    │
                    │  Workers  │    └─────────────┘                  └─────────────┘
                    │ Triggerer │
                    └───────────┘

  Infra local: Docker Compose
  ┌─────────────────────────────────────────────────────────┐
  │ postgres │ redis │ minio │ spark-master │ spark-worker  │
  └─────────────────────────────────────────────────────────┘
```

---

## Por que cada ferramenta

| Ferramenta | Decisão técnica |
|---|---|
| **Apache Airflow 2.8** | Orquestrador maduro com TaskGroups, XCom tipado, sensores rescheduláveis e amplo ecossistema de providers. CeleryExecutor para paralelismo real. |
| **Great Expectations 0.18** | Validação declarativa de dados com suites versionáveis em JSON, Data Docs auto-gerados e integração nativa com Spark. |
| **PySpark + Delta Lake** | Processamento distribuído com ACID transactions, time travel, e merge/upsert nativo para idempotência. |
| **MinIO** | Drop-in replacement do S3 para desenvolvimento local — mesma API, zero custo, sem dependência de cloud. |
| **GitHub Actions** | CI/CD integrado ao repositório, com jobs paralelos, cache de dependências e approval gates para produção. |
| **Structlog** | Logging estruturado (JSON) para observabilidade — filtrável, parsável por ferramentas como Datadog/ELK. |
| **Docker Compose** | Ambiente reprodutível com todos os serviços, healthchecks e volumes persistentes. |
| **Slack Webhooks** | Alertas em tempo real para falhas de task, SLA misses e resumo de execução. |

---

## Quick Start (5 comandos)

```bash
# 1. Clone o repositório
git clone https://github.com/YOUR_USER/dataops-pipeline.git && cd dataops-pipeline

# 2. Setup completo (env, Docker, GE, MinIO)
make setup

# 3. Baixe o dataset Olist e extraia na pasta data/
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/ --unzip

# 4. Faça upload dos CSVs para o MinIO
make ingest

# 5. Dispare a DAG
make trigger-dag
```

### Baixando o dataset manualmente

Se não tiver a CLI do Kaggle:
1. Acesse https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Clique em **Download** (requer conta Kaggle gratuita)
3. Extraia os CSVs na pasta `data/` do projeto

Para instalar a CLI do Kaggle:
```bash
pip install kaggle
# Configure ~/.kaggle/kaggle.json com seu token
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/ --unzip
```

---

## Comandos do Makefile

| Comando | Descrição |
|---|---|
| `make setup` | Bootstrap completo: .env, deps, Docker, GE, seed MinIO |
| `make up` | Sobe todos os containers |
| `make down` | Para todos os containers |
| `make logs` | Tail nos logs do scheduler |
| `make ingest` | Roda ingestão via spark-submit |
| `make transform` | Roda transformações Silver e Gold |
| `make validate` | Executa todos os checkpoints GE |
| `make test` | Roda pytest com output verboso |
| `make lint` | black + flake8 + mypy |
| `make docs` | Gera e disponibiliza Data Docs |
| `make clean` | Para containers, remove volumes e caches |
| `make trigger-dag` | Dispara a DAG dataops_pipeline |

---

## Estrutura do Projeto

```
dataops-pipeline/
├── .github/workflows/     # CI (lint+test) e Deploy (S3 sync)
├── dags/                  # DAG principal + utilities (alerts, GE operator)
├── expectations/          # Suites GE em JSON (bronze, silver, gold)
├── checkpoints/           # Checkpoints GE em YAML
├── great_expectations/    # Config GE + datasources
├── ingestion/             # SparkSession factory + ingestão Bronze
├── transformations/       # Silver (limpeza) + Gold (métricas)
├── tests/                 # DAG integrity, unit tests, GE suite validation
├── scripts/               # Bootstrap GE + seed MinIO
├── docker-compose.yml     # 10 serviços com healthchecks
├── Makefile               # 12 targets de automação
└── requirements*.txt      # Deps fixadas (prod + dev)
```

---

## Troubleshooting

### 1. `Connection refused` ao acessar MinIO (porta 9000)
- Verifique se o container MinIO está healthy: `docker compose ps`
- Aguarde o `minio-init` concluir a criação dos buckets
- Teste com: `curl http://localhost:9000/minio/health/live`

### 2. Airflow webserver não inicia (porta 8080)
- Verifique se o `airflow-init` completou: `docker compose logs airflow-init`
- Confirme que o Postgres está healthy antes do Airflow subir
- Verifique a `FERNET_KEY` no `.env` (gere com `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)

### 3. Spark não encontra Delta Lake jars
- Os jars são baixados automaticamente via `spark.jars.packages`
- Na primeira execução pode demorar ~2min para download
- Se falhar, verifique conectividade: `docker compose exec spark-master ping maven.org`

### 4. Testes falhando com `ModuleNotFoundError`
- Execute `pip install -r requirements.txt -r requirements-dev.txt`
- Verifique se o Java está instalado (necessário para PySpark): `java -version`
- No CI, o `actions/setup-java@v4` cuida disso automaticamente

### 5. DAG não aparece no Airflow UI
- Verifique se o volume `./dags` está montado: `docker compose exec airflow-webserver ls /opt/airflow/dags/`
- Erros de import aparecem em: Airflow UI → Admin → Import Errors
- Force um rescan: `docker compose exec airflow-webserver airflow dags reserialize`

---

## LinkedIn Post

> **Construí um pipeline DataOps completo e open-source — e aqui está o que aprendi**
>
> Nas últimas semanas, implementei um projeto end-to-end de DataOps usando o dataset público Brazilian E-Commerce (Olist) do Kaggle. O objetivo: demonstrar como aplicar práticas de engenharia de software de ponta ao ciclo de vida dos dados.
>
> **O que o projeto inclui:**
>
> - **Arquitetura Medallion (Bronze/Silver/Gold)** com Delta Lake para garantir ACID transactions e idempotência via merge/upsert
> - **Apache Airflow 2.8** com CeleryExecutor, TaskGroups, sensores rescheduláveis e callbacks de SLA — orquestração robusta de verdade
> - **Great Expectations** em todas as camadas: 3 suites com 25+ expectations validando schema, tipos, nulidade, unicidade e regras de negócio
> - **CI/CD com GitHub Actions**: lint (black + flake8 + mypy), testes com cobertura, validação de suites GE e deploy automatizado para S3
> - **Ambiente 100% local** com Docker Compose: Airflow, Spark, MinIO (S3 local), Postgres, Redis — tudo com healthchecks
> - **Alertas no Slack** para falhas, SLA misses e resumo de execução com estatísticas de validação
> - **Testes de verdade**: DAG integrity (8 checks), unit tests de transformações com PySpark, validação de suites GE
>
> **O que mais me ensinou:** DataOps não é só "colocar dados num warehouse". É tratar dados com o mesmo rigor que tratamos código — versionamento, testes, CI/CD, observabilidade e ambientes reprodutíveis.
>
> O projeto é open-source e pensado para ser referência para quem quer ir além do básico em engenharia de dados.
>
> **Stack:** Airflow | PySpark | Delta Lake | Great Expectations | Docker | GitHub Actions | MinIO | Structlog
>
> Link do repositório nos comentários.
>
> #DataOps #DataEngineering #ApacheAirflow #GreatExpectations #DeltaLake #OpenSource #EngenhariaDeDados

---

## Licença

MIT

