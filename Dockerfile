ARG PYTHON_VERSION=3.10
ARG APP_NAME=extraction_worker

FROM python:$PYTHON_VERSION as staging
ARG APP_NAME
ENV POETRY_VERSION=1.2.0
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /$APP_NAME
COPY ./poetry.lock ./pyproject.toml ./
COPY ./$APP_NAME ./

FROM staging as development
ARG APP_NAME
WORKDIR /$APP_NAME
RUN poetry install
CMD [ "poetry", "run", "python", "./extraction_worker.py"]

FROM development AS test
ARG cache=1
COPY ./tests ./tests

CMD ["poetry", "run", "pytest", "-q", "tests/"]

FROM staging as build
RUN poetry export --output=requirements.txt

FROM python:$PYTHON_VERSION as production
ARG APP_NAME
COPY --from=build /$APP_NAME/requirements.txt /$APP_NAME/requirements.txt
WORKDIR /$APP_NAME
RUN pip3 install -r requirements.txt
COPY ./$APP_NAME ./
CMD ["python", "./extraction_worker.py"]