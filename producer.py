import json
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime
import time
import config

df = pd.read_csv("data/twcs.csv", chunksize=5000)

producer_twits = KafkaProducer(bootstrap_servers=config.BOOTSTRAP_SERVERS,
                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer_users = KafkaProducer(bootstrap_servers=config.BOOTSTRAP_SERVERS,
                               value_serializer=lambda v: v.encode('utf-8'))


def on_send_error(excp):
    print('[ERROR]', excp)


print("Sending")
if __name__ == "__main__":
    counter = 0
    tracked_users = set()

    for chunk in df:
        chunk_iter = chunk.iterrows()
        for i in chunk_iter:
            elem = i[1]
            producer_twits.send("twits",
                                value=
                                {
                                    "author_id": elem.author_id,
                                    "created_at": str(datetime.now()),
                                    "text": elem.text
                                }
                                ).add_errback(on_send_error)

            if elem.author_id not in tracked_users:
                tracked_users.add(elem.author_id)
                producer_users.send('users', value=elem.author_id).add_errback(on_send_error)

            counter += 1
            if counter > 40:
                counter = 0
                time.sleep(1)

producer_twits.flush()
print("End sending")
