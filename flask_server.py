from flask import Flask
from utils.KafkaConsumerAdapter import KafkaConsumerAdapter
import config
from utils.S3Adapter import S3Adapter
import datetime

app = Flask(__name__)


@app.route('/accounts/', methods=["GET"])
def get_accounts():
    s3_adapter.upload_file(f"accounts-{str(datetime.datetime.now())}.json", adapter.list_accounts())
    return "OK"


@app.route('/popular_tweets/', methods=["GET"])
def get_popular_tweets():
    s3_adapter.upload_file(f"popular_tweets-{str(datetime.datetime.now())}.json", adapter.highest_tweets_number())
    return "OK"


@app.route('/statistics/', methods=["GET"])
def get_statistics():
    s3_adapter.upload_file(f"statistics-{str(datetime.datetime.now())}.json", adapter.aggregated_statistics())
    return "OK"


@app.route('/active_accounts/<int:n>', methods=["GET"])
def get_active_accounts(n):
    s3_adapter.upload_file(f"active_accounts-{str(datetime.datetime.now())}.json", adapter.most_productive(n))
    return "OK"


@app.route('/hashtags/<int:n>', methods=["GET"])
def get_hashtags(n):
    s3_adapter.upload_file(f"hashtags-{str(datetime.datetime.now())}.json", adapter.popular_hashtags(n))
    return "OK"


if __name__ == "__main__":
    adapter = KafkaConsumerAdapter(config.BOOTSTRAP_SERVERS)
    s3_adapter = S3Adapter(config.AWS_CREDENTIALS, "mmarkiian.s3")
    app.run(debug=True)
