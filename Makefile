.PHONY: install setup up down run clean lint

install:
	pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down

setup:
	python setup_exchanges.py

run:
	python run_demo.py

clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

lint:
	ruff check . --fix

# 分步启动（各开一个终端）
svc-payment:
	uvicorn services.payment_service:app --port $(shell grep API_PORT .env 2>/dev/null | cut -d= -f2 || echo 8000) --reload

svc-inventory:
	python services/inventory_service.py

svc-inventory-2:
	python services/inventory_service.py 2

svc-customs:
	python services/customs_service.py

svc-nlp:
	python services/nlp_service.py

svc-cv:
	python services/cv_service.py

svc-alert:
	python services/alert_service.py
