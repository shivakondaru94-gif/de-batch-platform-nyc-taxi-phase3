# Single entry point for the whole platform. Everything needed to reproduce
# the system on a clean machine is one of these targets.
#
# The intended experience on a new machine is exactly one command:
#
#     make up        on a 16 GB laptop
#     make up-lite   in a cloud editor (GitHub Codespaces, Cloud Shell)
#
# which generates credentials, builds the images, starts all nine services
# and blocks until every one of them reports healthy. No file has to be
# edited by hand and no setting has to be clicked in a user interface.

.PHONY: help init up up-lite wait down logs ps backfill quarter smoke diagram scan secrets-check config clean nuke

help:
	@echo "up            - generate secrets, build, start, wait until healthy"
	@echo "up-lite       - same, sized for a 16 GB cloud machine (Codespaces)"
	@echo "smoke         - end-to-end check: ingest, validate, aggregate, count"
	@echo "down          - stop the platform (data volumes preserved)"
	@echo "ps            - service status"
	@echo "logs          - follow logs (S=service to filter)"
	@echo "backfill      - ingest 12 months of trip data (M=YYYY-MM for one month)"
	@echo "quarter       - build features for one quarter (Q=2024Q1)"
	@echo "diagram       - regenerate the architecture PNG"
	@echo "scan          - Trivy vulnerability scan of every pinned image"
	@echo "secrets-check - fail if a credential was ever committed"
	@echo "config        - render the resolved compose configuration"
	@echo "clean         - stop and remove containers"
	@echo "nuke          - clean + delete all data volumes"

# ---------------------------------------------------------------------------
# Credentials are generated, never typed. A fresh clone therefore starts with
# unique strong secrets rather than with placeholder values that a hurried
# operator would leave in place.
# ---------------------------------------------------------------------------
define gen_secret
$(shell openssl rand -hex 16 2>/dev/null || head -c 24 /dev/urandom | base64 | tr -d '/+=' )
endef

init:
	@if [ -f .env ]; then \
		echo ".env already exists - leaving it untouched"; \
	else \
		sed -e "s|^MINIO_ROOT_PASSWORD=.*|MINIO_ROOT_PASSWORD=$(call gen_secret)|" \
		    -e "s|^LAKE_SECRET_KEY=.*|LAKE_SECRET_KEY=$(call gen_secret)|" \
		    -e "s|^WAREHOUSE_PASSWORD=.*|WAREHOUSE_PASSWORD=$(call gen_secret)|" \
		    -e "s|^AIRFLOW_DB_PASSWORD=.*|AIRFLOW_DB_PASSWORD=$(call gen_secret)|" \
		    -e "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=$(call gen_secret)|" \
		    .env.example > .env; \
		echo "generated .env with fresh random credentials"; \
		echo "Airflow login: admin / $$(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)"; \
	fi

# Overlay that shrinks the resource limits and runs a single worker.
LITE := -f docker-compose.yml -f docker-compose.lite.yml

up: init
	docker compose up -d --build
	@$(MAKE) --no-print-directory wait
	@echo ""
	@echo "Airflow      http://127.0.0.1:8080"
	@echo "MinIO        http://127.0.0.1:9001"
	@echo "Spark master http://127.0.0.1:8081"
	@echo "Warehouse    docker compose exec warehouse psql -U warehouse -d features"
	@echo ""
	@echo "Both DAGs are already unpaused. Run 'make smoke' to verify end to end."

# Same platform, smaller limits and one worker, for a cloud editor.
up-lite: init
	docker compose $(LITE) up -d --build
	@$(MAKE) --no-print-directory wait
	@echo ""
	@echo "Open the PORTS tab: Airflow 8080, MinIO 9001, Spark 8081"
	@echo "Airflow login: admin / $$(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)"
	@echo ""
	@echo "Next: make smoke"

# Blocks until every service with a healthcheck reports healthy, so a
# follow-on command never races against a still-starting dependency.
wait:
	@echo "waiting for services to become healthy..."
	@for i in $$(seq 1 60); do \
		unhealthy=$$(docker compose ps --format '{{.Service}} {{.Health}}' \
			| awk '$$2 != "healthy" && $$2 != "" { print $$1 }'); \
		if [ -z "$$unhealthy" ]; then echo "all services healthy"; exit 0; fi; \
		sleep 5; \
	done; \
	echo "TIMEOUT - still not healthy:"; docker compose ps; exit 1

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f $(S)

# Ingest a single month:  make backfill M=2024-01
# Ingest the default 12:  make backfill
M ?=
backfill:
ifeq ($(M),)
	@for m in 01 02 03 04 05 06 07 08 09 10 11 12; do \
		docker compose exec -T airflow-scheduler \
			python /opt/ingestion/ingest_tlc.py --month 2024-$$m ; \
	done
else
	docker compose exec -T airflow-scheduler \
		python /opt/ingestion/ingest_tlc.py --month $(M)
endif

Q ?= 2024Q1
quarter:
	docker compose exec -T airflow-scheduler airflow dags trigger quarterly_features

# ---------------------------------------------------------------------------
# End-to-end verification. Ingests one month, validates it, aggregates the
# quarter it belongs to, then reports what actually reached the feature store.
# This is the check that the platform runs unattended on a target machine.
# ---------------------------------------------------------------------------
SMOKE_MONTH   ?= 2024-01
SMOKE_QUARTER ?= 2024Q1
# -e HOME=/tmp: the image runs under a uid with no /etc/passwd entry, so the
# JVM resolves user.home to "?" and Ivy rejects the relative path it builds
# from it. spark.jars.ivy pins the dependency cache somewhere writable for
# the same reason.
SUBMIT = docker compose exec -T -e HOME=/tmp -e HADOOP_USER_NAME=spark spark-master spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--py-files /opt/jobs/common.py

smoke:
	@echo "== 1/4 ingest $(SMOKE_MONTH)"
	docker compose exec -T airflow-scheduler \
		python /opt/ingestion/ingest_tlc.py --month $(SMOKE_MONTH)
	@echo "== 2/4 validate (bronze -> silver)"
	$(SUBMIT) /opt/jobs/bronze_to_silver.py \
		--month $(SMOKE_MONTH) --batch-id smoke-$(SMOKE_MONTH)
	@echo "== 3/4 aggregate (silver -> gold)"
	$(SUBMIT) /opt/jobs/silver_to_gold.py \
		--quarter $(SMOKE_QUARTER) --batch-id smoke-$(SMOKE_QUARTER)
	@echo "== 4/4 what reached the feature store"
	@docker compose exec -T warehouse psql -U $$(grep ^WAREHOUSE_USER .env | cut -d= -f2) \
		-d $$(grep ^WAREHOUSE_DB .env | cut -d= -f2) -c \
		"SELECT count(*) AS feature_rows, count(DISTINCT pickup_zone_id) AS zones, \
		        min(window_start) AS first_window, max(window_start) AS last_window \
		   FROM ml.features_demand_hourly;"

diagram:
	python docs/make_architecture_diagram.py

# --- security verification -------------------------------------------------
IMAGES = quay.io/minio/minio:RELEASE.2024-09-13T20-26-02Z \
	quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z \
	postgres:16-alpine \
	bitnamilegacy/spark:3.5.6 \
	apache/airflow:2.9.3-python3.11

scan:
	@for img in $(IMAGES); do \
		echo "== $$img"; \
		docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
			aquasec/trivy:0.55.0 image --severity HIGH,CRITICAL \
			--ignore-unfixed --scanners vuln $$img ; \
	done

# A committed .env is the single most damaging mistake this project can make.
secrets-check:
	@if git ls-files --error-unmatch .env >/dev/null 2>&1; then \
		echo "FAIL: .env is tracked by git"; exit 1; \
	else \
		echo "OK: .env is not tracked"; \
	fi
	@if git log --all --name-only --pretty=format: | sort -u | grep -qx ".env"; then \
		echo "FAIL: .env appears in git history"; exit 1; \
	else \
		echo "OK: .env never committed"; \
	fi

config:
	docker compose config

clean:
	docker compose down --remove-orphans

nuke:
	docker compose down -v --remove-orphans
