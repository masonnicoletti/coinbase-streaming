import os
import json
import re
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import timedelta, datetime
from quixstreams import Application

kafka_broker = "localhost:19092"



def set_window(record_time):

    window_start = record_time.replace(minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(hours=1)

    return window_start, window_end



def parse_data(key, value):

    entry = {
        "type": key,
        "sequence": value.get("sequence"),
        "product_id": value.get("product_id"),
        "price": value.get("price"),
        "open_24h": value.get("open_24h"),
        "volume_24h": value.get("volume_24h"),
        "low_24h": value.get("low_24h"),
        "high_24h": value.get("high_24h"),
        "volume_30d": value.get("volume_30d"),
        "best_bid": value.get("best_bid"),
        "best_bid_size": value.get("best_bid_size"),
        "best_ask": value.get("best_ask"),
        "best_ask_size": value.get("best_ask_size"),
        "side": value.get("side"),
        "time": value.get("time"),
        "trade_id": value.get("trade_id"),
        "last_size": value.get("last_size") 
    }

    return entry



def write_parquet(data, window_start):

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    timestamp = window_start.strftime("%Y-%m-%d_%H")
    filename = f"./data/coinbase_{timestamp}.parquet"
    
    if os.path.exists(filename):
        existing_table = pq.read_table(filename)
        new_table = pa.concat_tables([existing_table, table])
        pq.write_table(new_table, filename)
    
    else:
        #df.to_parquet(filename, index=False)
        pq.write_table(table, filename)



def main():

    app = Application(
        broker_address=kafka_broker,
        loglevel="INFO",
        consumer_group="coinbase-consumer",
        auto_offset_reset="earliest"
    )

    buffer = []
    window_start = None

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

                    if key != "ticker":
                        continue

                    time_string = value.get("time")
                    record_time = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%fZ")

                    if window_start is None:
                        window_start, window_end = set_window(record_time)

                    if record_time >= window_end:
                        write_parquet(buffer, window_start)
                        buffer.clear()
                        window_start, window_end = set_window(record_time)
                    
                    entry = parse_data(key, value)
                    buffer.append(entry)

                    print(f"Key: {key}")
                    print(f"Value: {value}")
                    print(f"Record time: {record_time}")
                    print(f"Offset: {offset}")

                    consumer.store_offsets(message)
        
        except KeyboardInterrupt:
            write_parquet(buffer, window_start)
            print("\nExited Kafka consumer.")
        
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
