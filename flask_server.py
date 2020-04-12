from flask import Flask, jsonify

from KafkaConsumerAdapter import KafkaConsumerAdapter

app = Flask(__name__)


@app.route('/accounts/', methods=["GET"])
def get_accounts():
    print(adapter.list_accounts())
    return "OK"


@app.route('/popular_tweets/', methods=["GET"])
def get_popular_tweets():
    return jsonify(adapter.highest_tweets_number())


@app.route('/statistics/', methods=["GET"])
def get_statistics():
    return jsonify(adapter.aggregated_statistics())


@app.route('/active_accounts/', methods=["GET"])
def get_active_accounts():
    return jsonify(adapter.most_productive(2))


@app.route('/hashtags/', methods=["GET"])
def get_hashtags():
    return jsonify(adapter.popular_hashtags(2))

if __name__ == "__main__":
    adapter = KafkaConsumerAdapter(['localhost:9092', 'localhost:9093', 'localhost:9094'])
    app.run(debug=True)