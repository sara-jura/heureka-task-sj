import pika
import logging
import json
import os
import uuid
from typing import Dict

from common.postgres_dao import PostgresDAO
from common.log import setup_logging
from common.constants import OFFER_PARAMS_TABLE_NAME, QUEUE_NAME, EXTRACTED_FIELDS, LEGACY_FIELDS


class ExtractionWorker:
    """ Worker that consumes offer data from a RabbitMQ queue, extracts only relevant data and saves
    them into a Postgres database"""

    def __init__(self, postgres_dao: PostgresDAO) -> None:
        self.postgres_dao = postgres_dao
        self.rabbitmq_connection = None
        self.channel = None
        self.logger = setup_logging(log_level=logging.INFO, logger_name="offer_extraction_worker")

    def connect(self) -> None:
        """ Connect to RabbitMQ and Postgres"""

        self.postgres_dao.connect()
        self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST")))
        self.channel = self.rabbitmq_connection.channel()

    def disconnect(self):
        """ Disconnect from RabbitMQ and Postgres"""

        self.postgres_dao.close()
        self.rabbitmq_connection.close()

    def setup_target_table(self) -> None:
        """ Creates the target table for extracted data in case it doesn't exist """

        query = f"""CREATE TABLE IF NOT EXISTS {OFFER_PARAMS_TABLE_NAME} (
                  id uuid PRIMARY KEY,
                  legacy jsonb,
                  attributes jsonb
                );
                """
        self.postgres_dao.execute_query(query)

    def validate_offer_data(self, body: Dict) -> bool:
        """ A crude validation of offer data """

        # check if all necessary keys exist in the input json
        for field in EXTRACTED_FIELDS:
            if field not in body:
                self.logger.warning(f"Offer data missing the key '{field}' ({body}")
                return False

        for field in LEGACY_FIELDS:
            if field not in body["legacy"]:
                self.logger.warning(f"Offer legacy data missing the key '{field}' ({body}")
                return False

        # check if offer_id is a valid UUID, throws ValueError if it's not
        try:
            uuid.UUID(body["id"])
        except ValueError:
            self.logger.warning(f"Offer id not in the proper UUID format ('{body['id']}')")
            return False
        return True

    def process_offer(self, ch, method, properties, body) -> None:
        """ Callback function to process a single message from the queue """
        try:
            body = json.loads(body)
            if self.validate_offer_data(body):
                # if data passes validation save the offer data into the target table, if an offer with the same id
                # exists update the parameters
                query = f"""
                            INSERT INTO {OFFER_PARAMS_TABLE_NAME} (id, legacy, attributes) 
                            VALUES (
                                '{body['id']}', 
                                '{json.dumps(body['legacy'])}', 
                                '{json.dumps(body['attributes'])}'
                            )
                            ON CONFLICT (id) DO UPDATE SET legacy = excluded.legacy, attributes = excluded.attributes;
                        """
                self.postgres_dao.execute_query(query)
                self.logger.info(f"Successfully processed offer {body['id']}")
        except Exception as e:
            self.logger.warning(f"Something went wrong when processing a message: {str(e)}")

    def process_messages(self):
        """ Connect to the queue and process keep processing the offer data"""
        # connect to the database and queue
        self.connect()
        # create the target database table if it doesn't exist
        self.setup_target_table()
        # start consuming messages
        try:
            self.channel.queue_declare(queue=QUEUE_NAME)
            self.channel.basic_consume(queue=QUEUE_NAME, on_message_callback=self.process_offer, auto_ack=True)
            self.channel.start_consuming()
        finally:
            self.disconnect()


def main():
    # get connection info for postgres
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")

    # initialize the postgres data access object
    postgres_dao = PostgresDAO(db=db_name, username=db_user, password=db_pass, port=db_port, host=db_host)

    # initialize the extraction worker
    worker = ExtractionWorker(postgres_dao)
    # start the processing data
    worker.process_messages()


if __name__ == '__main__':
    main()
