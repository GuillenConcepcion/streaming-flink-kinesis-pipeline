.PHONY: help up down init-aws build build-docker test test-lambda lint terraform-validate submit generate-traffic clean logs

SHELL := /bin/bash
PROJECT_NAME := streaming_flink_kinesis_pipeline
DOCKER_COMPOSE := docker compose

help:
	@echo "=========================================================================="
	@echo "  ODYSSEUS STREAMING PIPELINE: APACHE FLINK & AWS KINESIS                "
	@echo "=========================================================================="
	@echo "  make up                 - Start LocalStack and Apache Flink cluster"
	@echo "  make down               - Stop all running containers and remove volumes"
	@echo "  make init-aws           - Initialize Kinesis, S3, SQS, SNS, DynamoDB mock"
	@echo "  make build              - Compile Java 17 Flink application with host Maven"
	@echo "  make build-docker       - Compile inside Docker (Zero Maven/Java pre-req)"
	@echo "  make test               - Execute Flink CEP state machine & window tests"
	@echo "  make test-lambda        - Execute Lambda idempotency & Moto unit tests"
	@echo "  make lint               - Execute static code analysis with Ruff"
	@echo "  make terraform-validate - Check formatting and validate Terraform IaC"
	@echo "  make submit             - Submit shaded JAR to Flink JobManager via REST"
	@echo "  make generate-traffic   - Run Python synthetic telemetry generator"
	@echo "  make logs               - Tail container logs"
	@echo "  make clean              - Remove build artifacts and temporary files"
	@echo "=========================================================================="

up:
	@echo "[*] Starting LocalStack and Apache Flink Cluster..."
	$(DOCKER_COMPOSE) up -d
	@echo "[+] Services started. Flink Dashboard: http://localhost:8081"
	@echo "[+] LocalStack Gateway: http://localhost:4566"

down:
	@echo "[*] Stopping containers..."
	$(DOCKER_COMPOSE) down -v

init-aws:
	@echo "[*] Initializing LocalStack AWS Resources..."
	python scripts/init_localstack.py

build:
	@echo "[*] Building Flink Java 17 Streaming Application..."
	cd flink-processor && mvn clean package -DskipTests=false

build-docker:
	@echo "[*] Compiling Flink Application inside Docker (maven:3.9-eclipse-temurin-17)..."
	docker run --rm \
		-v "$$(pwd)/flink-processor:/app" \
		-w /app \
		maven:3.9-eclipse-temurin-17 \
		mvn clean package -DskipTests=false
	@echo "[+] Docker build complete. Shaded JAR created in flink-processor/target/"

test:
	@echo "[*] Running Flink CEP and Window Operator Unit Tests..."
	cd flink-processor && mvn test

test-lambda:
	@echo "[*] Running AWS Lambda Alerter Idempotency Tests (Moto Mocking)..."
	pytest lambda-alerter/tests/ -v

lint:
	@echo "[*] Executing Ruff Linting on Python Source Files..."
	ruff check generator/ lambda-alerter/

terraform-validate:
	@echo "[*] Validating Terraform Infrastructure as Code..."
	cd terraform && terraform fmt -check && terraform init -backend=false && terraform validate

submit:
	@echo "[*] Submitting Flink Job to JobManager (http://localhost:8081)..."
	@JAR_FILE=$$(ls flink-processor/target/flink-processor-*.jar 2>/dev/null | grep -v "original" | head -n 1); \
	if [ -z "$$JAR_FILE" ]; then \
		echo "[!] Error: No shaded JAR found. Run 'make build' or 'make build-docker' first."; exit 1; \
	fi; \
	echo "[*] Uploading $$JAR_FILE..."; \
	JAR_ID=$$(curl -s -X POST -H "Expect:" -F "jarfile=@$$JAR_FILE" http://localhost:8081/jars/upload | grep -o '"filename":"[^"]*' | cut -d'"' -f4 | rev | cut -d'/' -f1 | rev); \
	echo "[*] Jar uploaded with ID: $$JAR_ID. Submitting execution..."; \
	curl -s -X POST "http://localhost:8081/jars/$$JAR_ID/run" | tee /dev/tty
	@echo "\n[+] Job submitted successfully."

generate-traffic:
	@echo "[*] Launching IoT Telemetry Generator (Normal + Anomaly Injection)..."
	python generator/generator.py --rate 1000 --anomaly-interval 45

logs:
	$(DOCKER_COMPOSE) logs -f

clean:
	@echo "[*] Cleaning build artifacts..."
	cd flink-processor && mvn clean || true
	rm -rf generator/__pycache__ lambda-alerter/__pycache__ lambda-alerter/tests/__pycache__ .pytest_cache
