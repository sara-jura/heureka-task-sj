.PHONY:*


install-requirements:
	poetry install --no-root

clean:
	docker compose rm -vf

build:
	docker compose build

run:
	docker compose up

build-test:
	docker compose -f compose-test.yaml  build

run-test:
	docker compose -f compose-test.yaml  up

format:
	isort . --profile "black"
	black .

