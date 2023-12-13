.PHONY:*

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

# for easier development without docker you can create a virtual environment, install poetry and use it to install
#d dependencies and for code formatting
initialize-venv:
	python3 -m venv venv
	source venv/bin/activate

install-poetry:
    curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -


install-requirements:
	poetry install --no-root

format:
	poetry run isort . --profile "black"
	poetry run black .

