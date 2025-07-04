#!/usr/bin/env python
import os
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer
from dotenv import load_dotenv

if __name__ == '__main__':
    # Load environment variables from .env file in current or parent directory
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration file.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)

    config = dict(config_parser['default'])
    if config_parser.has_section('consumer'):
        config.update(config_parser['consumer'])

    # Override sensitive values with environment variables
    config['bootstrap.servers'] = os.getenv('BOOTSTRAP_SERVERS', config.get('bootstrap.servers'))
    config['sasl.username'] = os.getenv('SASL_USERNAME', config.get('sasl.username'))
    config['sasl.password'] = os.getenv('SASL_PASSWORD', config.get('sasl.password'))

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "thermostat_readings"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                print(f"Consumed event from topic {msg.topic()}: key = {msg.key()} value = {msg.value()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
