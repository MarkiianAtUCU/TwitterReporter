from datetime import timedelta, datetime
from utils.utils import n_max_messsages, most_popular_hashtags, time_array, aggregate_statistics
from kafka import KafkaConsumer, TopicPartition
import json


class KafkaConsumerAdapter:
    tweets_partition = TopicPartition(topic="twits", partition=0)
    users_partition = TopicPartition(topic="users", partition=0)

    def __init__(self, bootstrap_servers):
        self.consumer_twits = KafkaConsumer('twits', enable_auto_commit=False, auto_offset_reset='earliest',
                                            bootstrap_servers=bootstrap_servers,
                                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

        self.consumer_users = KafkaConsumer('users', enable_auto_commit=False, auto_offset_reset='earliest',
                                            bootstrap_servers=bootstrap_servers,
                                            value_deserializer=lambda m: m.decode('utf-8'))

    def list_accounts(self):
        last_offset = self.consumer_users.end_offsets([self.users_partition])[self.users_partition] - 1
        result = []
        for i in self.consumer_users:
            result.append(
                {
                    "author_id": i.value
                }
            )
            if last_offset == i.offset:
                break
        return result

    def _fetch_all_messages(self, n_hours):
        first_twit_time = datetime.now() - timedelta(hours=n_hours)
        first_offset = self.consumer_twits.offsets_for_times(
            {self.tweets_partition: int(datetime.timestamp(first_twit_time) * 1000)}
        )[self.tweets_partition]

        if first_offset is None:
            return dict()
        else:
            first_offset = first_offset.offset - 1

        last_offset = self.consumer_twits.end_offsets([self.tweets_partition])[self.tweets_partition] - 1

        self.consumer_twits.seek(self.tweets_partition, first_offset)

        twits_by_author = dict()

        for i in self.consumer_twits:
            if i.value['author_id'] in twits_by_author:
                twits_by_author[i.value['author_id']].append(i.value)
            else:
                twits_by_author[i.value['author_id']] = [i.value]
            if last_offset == i.offset:
                break

        return twits_by_author

    def _fetch_all_messages_from_to(self, first_twit_time, last_twit_time):
        first_offset = self.consumer_twits.offsets_for_times(
            {self.tweets_partition: int(datetime.timestamp(first_twit_time) * 1000)}
        )[self.tweets_partition]

        if first_offset is None:
            return dict()
        else:
            first_offset = first_offset.offset - 1

        last_offset = self.consumer_twits.offsets_for_times(
            {self.tweets_partition: int(datetime.timestamp(last_twit_time) * 1000)}
        )[self.tweets_partition]

        if last_offset is None:
            last_offset = self.consumer_twits.end_offsets([self.tweets_partition])[self.tweets_partition] - 1
        else:
            last_offset = last_offset.offset - 1

        self.consumer_twits.seek(self.tweets_partition, first_offset)

        twits_by_author = dict()

        for i in self.consumer_twits:
            if i.value['author_id'] in twits_by_author:
                twits_by_author[i.value['author_id']].append(i.value)
            else:
                twits_by_author[i.value['author_id']] = [i.value]
            if last_offset == i.offset:
                break
        return twits_by_author

    def highest_tweets_number(self):
        twits_by_author = self._fetch_all_messages(3)
        most_productive_tweetes = n_max_messsages(twits_by_author, 10)

        result = []
        for user, _ in most_productive_tweetes:
            result.append(
                {
                    "author_id": user,
                    "tweets": twits_by_author[user][:-(10 + 1):-1]
                }
            )
        return result

    def aggregated_statistics(self):
        num_hours = 3
        result = []
        messages_by_time = []
        timestamps = time_array(datetime.now(), num_hours)
        for i in range(1, num_hours + 1):
            messages_by_time.append(
                self._fetch_all_messages_from_to(timestamps[i - 1], timestamps[i])
            )

        statistics = aggregate_statistics(messages_by_time)
        for user in statistics:
            result.append(
                {
                    "author_id": user,
                    "tweets_number_per_hour": statistics[user]
                }
            )
        return result

    def most_productive(self, n):
        result = []
        twits_by_author = self._fetch_all_messages(n)
        most_productive_tweetes = n_max_messsages(twits_by_author, 20)
        for user, tweets_num in most_productive_tweetes:
            result.append(
                {
                    "author_id": user,
                    "tweets_number": tweets_num
                }
            )
        return result

    def popular_hashtags(self, n):
        result = []
        twits_by_author = self._fetch_all_messages(n)
        hashtags_dict = most_popular_hashtags(twits_by_author)
        for hashtag in hashtags_dict:
            result.append(
                {
                    "hashtag": hashtag,
                    "num_occurrences": hashtags_dict[hashtag]
                }
            )
        return sorted(result, key=lambda o: o["num_occurrences"], reverse=True)
