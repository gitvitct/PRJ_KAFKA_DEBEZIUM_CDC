import argparse
from kafka_consumer import create_consumer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group", required=True)

    args = parser.parse_args()

    consumer = create_consumer(args.group)
    consumer.subscribe([args.topic])

    print(f"Consuming topic: {args.topic}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(msg.error())
            continue

        print(msg.value())

if __name__ == "__main__":
    main()