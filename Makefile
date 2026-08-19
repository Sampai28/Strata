# Strata targets.
#
# MSYS_NO_PATHCONV matters on Windows and is not optional. Under Git Bash, MSYS
# rewrites anything that looks like a Unix path in a command argument into a
# Windows path, so `docker exec ... hdfs dfs -ls /strata` arrives inside the
# container as `C:/Program Files/Git/strata` and fails with the memorable
# "No FileSystem for scheme C". Exporting this for the whole file disables that
# rewriting. On Linux and macOS the variable is simply ignored.
export MSYS_NO_PATHCONV = 1

CONFIG      ?= configs/smoke.yaml
TIER        ?= configs/tier.env
COMPOSE     := docker compose --env-file $(TIER) -f docker/docker-compose.yml
CLIENT      := strata-spark-client
NAMENODE    := strata-namenode

# Config is passed into the container by path, not by value, so the job reads
# exactly the file in the repo rather than a copy of its parsed contents.
CONTAINER_CONFIG := /opt/strata/$(CONFIG)
SUBMIT := docker exec \
	-e STRATA_CONFIG=$(CONTAINER_CONFIG) \
	-e PYTHONPATH=/opt/strata/src \
	$(CLIENT) /opt/spark/bin/spark-submit --master spark://spark-master:7077

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------

## up: start the cluster and wait for HDFS to leave safe mode
up:
	$(COMPOSE) up -d
	@echo "waiting for HDFS..."
	@until docker exec $(NAMENODE) hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; do sleep 2; done
	@docker exec $(NAMENODE) hdfs dfs -mkdir -p /spark-events /strata
	@docker exec $(NAMENODE) hdfs dfs -chmod -R 777 /spark-events /strata
	@echo "cluster ready:"
	@echo "  NameNode      http://localhost:9870"
	@echo "  Spark master  http://localhost:8080"
	@echo "  HiveServer2   beeline -u jdbc:hive2://localhost:10000"

## up-history: start the cluster including the Spark History Server
up-history:
	$(COMPOSE) --profile history up -d
	@echo "  History       http://localhost:18080"

## down: stop the cluster, keeping volumes and images
down:
	$(COMPOSE) down

## clean: stop the cluster and delete the HDFS and Postgres volumes
clean:
	$(COMPOSE) down -v

## destroy: clean, and remove the images built by this project as well
destroy:
	$(COMPOSE) down -v --rmi local

## ps: service status
ps:
	$(COMPOSE) ps

## logs: follow logs for all services
logs:
	$(COMPOSE) logs -f --tail=100

## shell: a shell inside the Spark client container
shell:
	docker exec -it $(CLIENT) bash

## beeline: connect to HiveServer2
beeline:
	docker exec -it strata-hiveserver2 beeline -u jdbc:hive2://localhost:10000

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

## generate: build the synthetic star schema at $(CONFIG) scale
generate:
	$(SUBMIT) /opt/strata/src/strata/generate/generator.py

## quality: run raw -> curated through the validation and reconciliation gates
quality:
	$(SUBMIT) /opt/strata/src/strata/ingest/curate.py

## load: alias for quality; raw -> curated is the load
load: quality

## exp1: partitioning strategy
exp1:
	$(SUBMIT) /opt/strata/src/strata/experiments/exp1_partitioning.py

## exp2: file format and compression codec
exp2:
	$(SUBMIT) /opt/strata/src/strata/experiments/exp2_formats.py

## bench-smoke: the whole suite at smoke scale
bench-smoke: generate quality exp1 exp2 hdfs-report
	@echo "smoke suite complete; raw measurements in results/"

## bench-full: the whole suite at full scale. Deliberately explicit.
## Read docs/BUILD_NOTES.md first: this wants far more memory, disk and time.
bench-full:
	$(MAKE) bench-smoke CONFIG=configs/full.yaml

# ---------------------------------------------------------------------------
# HDFS
# ---------------------------------------------------------------------------

## hdfs-report: cluster report, fsck, and per-path sizes
hdfs-report:
	@echo "=== dfsadmin -report ==="
	@docker exec $(NAMENODE) hdfs dfsadmin -report | head -12
	@echo
	@echo "=== fsck /strata ==="
	@docker exec $(NAMENODE) hdfs fsck /strata | grep -E "Status|Total (size|files|blocks)|Average block|replicated"
	@echo
	@echo "=== du -h /strata ==="
	@docker exec $(NAMENODE) hdfs dfs -du -h /strata

## hdfs-ls: recursive listing of the experiment output
hdfs-ls:
	@docker exec $(NAMENODE) hdfs dfs -ls -R /strata/experiments | head -40

## hdfs-count: directory, file and byte counts per experiment variant
hdfs-count:
	@docker exec $(NAMENODE) hdfs dfs -count /strata/experiments/exp1/* /strata/experiments/exp2/*

## hdfs-blocks: block-level detail. The average block size against the 128MB
## block size is the small-file measure.
hdfs-blocks:
	@docker exec $(NAMENODE) hdfs fsck /strata -files -blocks | tail -20

.PHONY: up up-history down clean destroy ps logs shell beeline \
        generate quality load exp1 exp2 bench-smoke bench-full \
        hdfs-report hdfs-ls hdfs-count hdfs-blocks help

## help: list targets
help:
	@echo "Strata (CONFIG=$(CONFIG), TIER=$(TIER))"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
