.PHONY: setup run-pipeline run-dashboard test lint format docker-build docker-run deploy clean

setup:
	uv sync

run-pipeline:
	uv run python -m pipeline.run

run-dashboard:
	uv run streamlit run dashboard/app.py --server.port 8501

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check . && uv run ruff format --check .

format:
	uv run ruff format .

docker-build:
	docker build -t arqtic .

docker-run:
	docker run -p 8501:8080 arqtic

deploy:
	cd terraform && terraform init && terraform apply

clean:
	rm -rf ./data/ .cache*
