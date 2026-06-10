from src.consumer.kafka_consumer import CDCConsumer


def main():

    consumer = CDCConsumer()

    consumer.run()


if __name__ == "__main__":
    main()