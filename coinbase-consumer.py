import json
import re
import time
import pandas as pd
from datetime import timedelta, datetime
from quixstreams import Application

kafka_broker = "localhost:19092"



def set_window():

    window_start = datetime.now().replace(minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(hours=1)
    return window_start, window_end



def parse_data(key, value):

    entry = {
        "type": key,
        "sequence": value["sequence"],
        "product_id": value["product_id"],
        "price": value["price"],
        "open_24h": value["open_24h"],
        "volume_24h": value["volume_24h"],
        "low_24h": value["low_24h"],
        "high_24h": value["high_24h"],
        "volume_30d": value["volume_30d"],
        "best_bid": value["best_bid"],
        "best_bid_size": value["best_bid_size"],
        "best_ask": value["best_ask"],
        "best_ask_size": value["best_ask_size"],
        "side": value["side"],
        "time": value["time"],
        "trade_id": value["trade_id"],
        "last_size": value["last_size"] 
    }

    return entry



def write_parquet(data, window_start):

    df = pd.DataFrame(data)
    timestamp = window_start.strftime("%Y-%m-%d_%H")
    filename = f"data/coinbase_{timestamp}.parquet"
    df.to_parquet(filename, index=False)



def main():

    app = Application(
        broker_address=kafka_broker,
        loglevel="INFO",
        consumer_group="coinbase-consumer",
        auto_offset_reset="earliest"
    )


    window_start, window_end = set_window()
    buffer = []

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

                    record_time = datetime.now()

                    if record_time >= window_end:
                        write_parquet(buffer, window_start)
                        buffer.clear()
                        window_start, window_end = set_window()

                    print(f"Key: {key}")
                    print(f"Value: {value}")
                    print(f"Offset: {offset}")
                    print(f"Record time: {record_time}")
                    print(f"Window start: {window_start}")
                    print(f"Window end: {window_end}")

                    if key == "ticker":
                        entry = parse_data(key, value)
                        buffer.append(entry)

                    consumer.store_offsets(message)
        
        except KeyboardInterrupt:
            print("\nExited Kafka consumer.")
        
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
