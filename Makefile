up:
	docker compose --env-file .env.dev -f docker/docker-compose.yml up -d
build:
	docker compose --env-file .env.dev -f docker/docker-compose.yml up --build -d

down:
	docker compose --env-file .env.dev -f docker/docker-compose.yml down

remove:
	docker compose --env-file .env.dev -f docker/docker-compose.yml down -v

build-code:
	docker compose --env-file .env.dev -f docker/docker-compose.yml up --build dagster_user_code -d

copy-data:
	docker cp dagster_user_code:/data/duckdb/premierlytics.duckdb ~/Projects/premierlytics-data-platform/data/premierlytics.duckdb

invoke-aws-lambda:
	aws lambda invoke \
  --function-name premierlytics-start-pipeline \
  --payload '{}' \
  --region eu-west-1 \
  --profile premierlytics \
  response.json