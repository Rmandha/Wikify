from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json
from time import sleep
from urllib3.exceptions import ProtocolError
from collections import defaultdict

access_token = "<your-access-token>"
access_token_secret = "<your-access-token-secret>"
consumer_key = "<your-consumer-key>"
consumer_secret = "<your-consumer-secret>"


# The Kafka topic we will insert the messages into
topic = "allTweets"


class TweetListener(StreamListener):

        def on_data(self, data):
            json_data = json.loads(data)
            rt = json_data.get('retweeted_status')

            if rt is not None:
                if rt.get('extended_tweet') is not None:
                    # We are using a keyed producer with
                    producer.send(topic, key=str(json_data['created_at']),
                              value=rt['extended_tweet']['full_text'].encode('ascii', 'replace'))
                else:
                    producer.send(topic, key=str(json_data['created_at']),
                              value=rt['text'].encode('ascii', 'replace'))
            else:
                if json_data.get('extended_tweet') is None:
                    producer.send(topic, key=str(json_data.get('created_at',' ')),
                              value=json_data.get('text','').encode('ascii', 'replace'))
                else:
                    producer.send(topic, key=str(json_data['created_at']),
                              value=json_data['extended_tweet']['full_text'].encode('ascii', 'replace'))

            return True

        def on_error(self, statuscode):
            print("Error Code: %s" %statuscode)
            return True


# We have setup a local cluster of 3 broker nodes on three different ports - 9092, 9093, 9094
# producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'])

# I have created a Kafka Cluster of 3 broker nodes on AWS with 3 EC2 nodes
producer = KafkaProducer(bootstrap_servers=['ec2-52-3-61-194.compute-1.amazonaws.com:9092',
                                            'ec2-52-90-17-194.compute-1.amazonaws.com:9092',
                                            'ec2-52-20-17-194.compute-1.amazonaws.com:9092'])

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, TweetListener())

words_to_track = "a the to and is in it you of for on my that at with me do have just this be so are " \
                     "not was but out up what now new from your like good no get all about we if time as day will one how can some an am " \
                     "by going they go or has know today there love more work too got he back think did when see really had" \
                     " great off would need here thanks been still people who night want why home should well much then " \
                     "right make last over way does getting watching its only her post his morning very she them could " \
                     "first than better after tonight our again down news man us tomorrow best into any hope week nice " \
                     "show yes where take check come fun say next watch never bad free life".split()

while True:
    try:
        stream.filter(languages=["en"], track=words_to_track)
        sleep(.5)
    except (ProtocolError, AttributeError):
        continue
