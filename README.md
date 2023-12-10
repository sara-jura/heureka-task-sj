# heureka-task

Solution of a task for applicant

# Task purpose:

We need one microservice. The service will parse messages from a queue in RabbitMQ and store certain parts of them in a
database.
The messages are JSON objects encoded in a binary format and represent an offer entity.
We need to extract the values under the keys `legacy` and `attributes` from the message and store them in the DB.

# Task solution:


## Project structure

```
.
├── extraction_worker               # source code for the extractor
│   ├── common
│   │   ├── constants.py            # keeps main constants in one place
│   │   ├── postgres_dao.py         # contains a DAO class for postgres db interaction
│   │   └── log.py                  # logger setup
│   ├── Dockerfile                  # build the image for an extraction worker
│   └── extraction_worker.py        # the main source code with woker for extraction
├── test/                           # the test and test data
├── compose.yaml
└── README.md
```

## Dependencies
* [pika](https://pypi.org/project/pika/)
* [psycopg2](https://pypi.org/project/psycopg2/)


## extraction_worker:

The main source code is in `extraction_worker/extraction_worker.py`. It contains an ExtractionWorker class that
encapsulates
the entire ETL process. An `ExtractionWorker` instance has to be initialized with an instance of `PostgresDAO` - a data
access object that abstract the main code from the database-specific functions. The method `process_messages` runs in an
infinite loop, reading messages from RabbitMQ, processing them and saving the results in a Postgres database.

## Database details

The target table in the Postgres database into which the offer data is stored is `offer_parameters` and has the
following columns:

| column     | type  |
|------------|-------|
| id         | UUID  |
| legacy     | jsonb |
| attributes | jsonb |

The input offer data is validated to make sure they contain the necessary fields and if the id has the correct UUID
structure. The values under the keys id, legacy and attributes are then saved into the table as-is, keeping their
original structure.

Note: If an offer with an id that's already present in the table is encountered, the data is overwritten.

## Running the code

### Using Docker Compose

You will need Docker installed to follow the next step. To create and run the image use the following command:

```bash
docker-compose up --build
```

Stop the run with:

```bash
docker-compose down
```

### Running the tests

The `tests\` directory contains tests and test data, to test the ExtractionWorker. In order for the tests to work, you
need to have a local instance of RabbitMQ and Postgres running. You may once again use Docker compose:

```bash
docker-compose up -d rabbitmq db
```

Alternatively you can run them in your own way and change the values in the `env_dict` variable
within `test_extractionworker.py` as needed.

You can run the test using:

```bash
python -m pytest -q tests/
```
