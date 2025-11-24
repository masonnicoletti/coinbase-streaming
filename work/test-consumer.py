import json
import time
from quixstreams import Application

kafka_broker = "localhost:19092"

def main():

    app = Application(
        broker_address=kafka_broker,
        loglevel="INFO",
        consumer_group="coinbase-consumer",
        auto_offset_reset="earliest"
    )

    with app.get_consumer() as consumer:

        consumer.subscribe(['crypto-stream'])
        print("Consumer started.")

        try:
            while True:
                message = consumer.poll(5)

                if message is None:
                    print("Waiting for message.")
                
                elif message.error() is not None:
                    raise Exception(message.error())
                
                else:
                    key = message.key().decode("utf-8")
                    value = json.loads(message.value())
                    offset = message.offset()

                    print(f"Key: {key}")
                    print(f"Value: {value}")
                    print(f"Offset: {offset}")

                    consumer.store_offsets(message)
        
        except KeyboardInterrupt:
            print("Exited Kafka consumer.")
        
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
