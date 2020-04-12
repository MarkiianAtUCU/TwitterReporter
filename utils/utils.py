import re
from datetime import timedelta

hashtag_re = re.compile("(?:^|\s)[＃#]{1}(\w+)", re.UNICODE)


def n_max_messsages(dictionary, n):
    final_list = [("dummy", float("inf"))] + [("0", 0) for _ in range(n)]
    used_set = set()
    first_element = next(iter(dictionary))
    for i in range(1, n + 1):
        final_list[i] = (first_element, len(dictionary[first_element]))
        for j in dictionary:
            if j not in used_set and len(dictionary[j]) > final_list[i][1]:
                final_list[i] = (j, len(dictionary[j]))
        used_set.add(final_list[i][0])

    del final_list[0]
    return final_list


def most_popular_hashtags(dictionary):
    hashtag_dictionary = dict()
    for user in dictionary:
        for twit in dictionary[user]:
            for hashtag in hashtag_re.findall(twit["text"]):
                if hashtag in hashtag_dictionary:
                    hashtag_dictionary[hashtag] += 1
                else:
                    hashtag_dictionary[hashtag] = 1
    return hashtag_dictionary


def time_array(from_hour, n_hours):
    result = [from_hour]
    for i in range(n_hours):
        result.append(result[-1] - timedelta(hours=1))

    return result[::-1]


def aggregate_statistics(array):
    result = dict()

    for i in range(len(array)):
        for user in array[i]:
            if user not in result:
                result[user] = [0 for _ in range(len(array))]
            result[user][i] = len(array[i][user])

    return result
