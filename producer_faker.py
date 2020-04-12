import json
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timedelta

df = pd.read_csv("data/twcs.csv", chunksize=5000)

producer_twits = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer_users = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                               value_serializer=lambda v: v.encode('utf-8'))

tracked_users = set()


def on_send_error(excp):
    print('[ERROR]', excp)


print("Start sending")
if __name__ == "__main__":

    counter = 0
    global_counter = 0
    time_counter = datetime.now() - timedelta(hours=5)

    for chunk in df:
        chunk_iter = chunk.iterrows()
        for i in chunk_iter:
            elem = i[1]
            producer_twits.send("twits", value=
            {
                "author_id": elem.author_id,
                "created_at": str(time_counter),
                "text": elem.text
            },
                                timestamp_ms=int(datetime.timestamp(time_counter) * 1000)
                                ).add_errback(on_send_error)
            if elem.author_id not in tracked_users:
                tracked_users.add(elem.author_id)
                producer_users.send('users', value=elem.author_id).add_errback(on_send_error)

            counter += 1
            global_counter += 1
            if counter > 35:
                counter = 0
                time_counter += timedelta(seconds=1)

            if time_counter > datetime.now():
                print("Finished sending")
                print(global_counter)
                break
        else:
            continue
        break

producer_twits.flush()
print("End sending")
