# Variabel untuk mempermudah perubahan path
DC_1 := ./airflow/docker-compose.yaml
DC_2 := ./clickhouse/docker-compose.yml
DC_3 := ./minio/docker-compose.yml
DC_4 := ./superset/docker-compose.yml

.PHONY: up down restart status

# Target utama untuk menjalankan semua secara berurutan
up:
	@echo "Starting airflow..."
	docker compose -f $(DC_1) up --build -d
	@sleep 10
	@echo "Starting clickhouse..."
	docker compose -f $(DC_2) up -d
	@sleep 10
	@echo "Starting minio..."
	docker compose -f $(DC_3) up -d
	@sleep 10
	@echo "Starting superset..."
	docker compose -f $(DC_4) up -d
	@echo "All systems GO!"

# Target untuk mematikan semua service
down:
	docker compose -f $(DC_4) down
	@sleep 5
	docker compose -f $(DC_3) down
	@sleep 5
	docker compose -f $(DC_2) down
	@sleep 5
	docker compose -f $(DC_1) down
	@sleep 5
	@echo "All systems stopped."

# Target untuk melihat status container
status:
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"