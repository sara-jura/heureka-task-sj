import pytest
import json
import pika
from extraction_worker.extraction_worker import ExtractionWorker
from extraction_worker.common.postgres_dao import PostgresDAO
from extraction_worker.common.constants import OFFER_PARAMS_TABLE_NAME, QUEUE_NAME
from tests.test_data import *
from unittest import mock
import os

env_dict = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_DB": "parameters",
    "POSTGRES_USER": "user_name",
    "POSTGRES_PASSWORD": "user_password",
    "POSTGRES_PORT": "5432",
    "RABBITMQ_HOST": "localhost",
}


@pytest.fixture
def mock_postgres_dao():
    print("\nSetting up database...")
    postgres_dao = PostgresDAO(
        db=env_dict["POSTGRES_DB"],
        username=env_dict["POSTGRES_USER"],
        password=env_dict["POSTGRES_PASSWORD"],
        port=env_dict["POSTGRES_PORT"],
        host=env_dict["POSTGRES_HOST"])
    postgres_dao.connect()
    # make sure the target table exists
    query = f"""CREATE TABLE IF NOT EXISTS {OFFER_PARAMS_TABLE_NAME} (
                      id uuid PRIMARY KEY,
                      legacy jsonb,
                      attributes jsonb
                    );
                    """
    postgres_dao.execute_query(query)
    delete_query = f"DELETE FROM {OFFER_PARAMS_TABLE_NAME}"
    postgres_dao.execute_query(delete_query)
    yield postgres_dao  # Provide the data to the test
    # Teardown: Clean up resources (if any) after the test
    print("\nTearing down resources...")
    delete_query = f"DELETE FROM {OFFER_PARAMS_TABLE_NAME}"
    postgres_dao.execute_query(delete_query)
    postgres_dao.close()


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with mock.patch.dict(os.environ, env_dict):
        yield


@pytest.fixture()
def mock_extraction_worker():
    postgres_dao = PostgresDAO(
        db=env_dict["POSTGRES_DB"],
        username=env_dict["POSTGRES_USER"],
        password=env_dict["POSTGRES_PASSWORD"],
        port=env_dict["POSTGRES_PORT"],
        host=env_dict["POSTGRES_HOST"])
    mock_extraction_worker = ExtractionWorker(postgres_dao)
    mock_extraction_worker.connect()
    yield mock_extraction_worker


@pytest.fixture()
def mock_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST")))
    with open('tests/mock_offers.json') as test_data_file:
        offers = json.load(test_data_file)["offers"]
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    for offer in offers:
        channel.basic_publish(exchange='',
                              routing_key=QUEUE_NAME,
                              body=json.dumps(offer))
    queue_state = channel.queue_declare(queue=QUEUE_NAME)
    init_queue_size = queue_state.method.message_count
    connection.close()
    yield None


@pytest.mark.parametrize("offer_data,is_valid", [
    (correct_offer, True),
    (missing_id, False),
    (missing_platformId, False),
    (wrong_UUID, False)
])
def test_validate_offer_data(mock_extraction_worker, offer_data, is_valid):
    """ Test if the validation of offers catches wrong data"""
    assert mock_extraction_worker.validate_offer_data(offer_data) is is_valid


def test_process_messages(mock_rabbitmq, mock_postgres_dao, mock_extraction_worker, ):
    """ Test how messages produced by mock_rabbitmq using the mock_offers.json data are handled"""
    # works with the assumption that mock_offers.json contains at least some valid offers, database should be empty
    # at the start and have a non-zero amount of row at the end
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST")))
    channel = connection.channel()
    queue_state = channel.queue_declare(queue=QUEUE_NAME)
    init_queue_size = queue_state.method.message_count
    assert init_queue_size > 0
    while True:
        queue_state = channel.queue_declare(queue=QUEUE_NAME)
        queue_empty = queue_state.method.message_count == 0
        if not queue_empty:
            method, properties, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
            mock_extraction_worker.process_offer(channel, method, properties, body)
        else:
            break
    assert mock_extraction_worker.postgres_dao.count_rows("select count(*) from offer_parameters") > 0


def test_process_offer(mock_postgres_dao, mock_extraction_worker):
    """ Test if orders are correctly added to the database """
    # check table doesn't already contain offer with given id
    select_query = f"select * from {OFFER_PARAMS_TABLE_NAME} where id = \'{correct_offer['id']}\'"
    rows = mock_postgres_dao.fetch_dicts(select_query)
    assert len(rows) == 0

    # check if the offer is added once processed
    mock_extraction_worker.process_offer(None, None, None, json.dumps(correct_offer))
    rows = mock_postgres_dao.fetch_dicts(select_query)
    assert len(rows) == 1

    # check if an offer with the same id overwrites the data
    mock_extraction_worker.process_offer(None, None, None, json.dumps(correct_offer_changed_country))
    assert correct_offer["legacy"]["countryCode"] != correct_offer_changed_country["legacy"]["countryCode"]
    rows = mock_postgres_dao.fetch_dicts(select_query)
    assert len(rows) == 1
    assert rows[0]["legacy"]["countryCode"] == correct_offer_changed_country["legacy"]["countryCode"]
