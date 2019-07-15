import os
import time 
import json
from textblob import TextBlob
from google.cloud import pubsub_v1
from dotenv import load_dotenv
load_dotenv()

import warnings 
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


subscriber = pubsub_v1.SubscriberClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    topic=os.getenv('TOPIC_NAME'),  # Set this to something appropriate.
)
subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    sub=os.getenv('SUBSCRIPTION_NAME'),  # Set this to something appropriate.
)
##subscriber.create_subscription(
##    name=subscription_name, topic=topic_name)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    testimonial = TextBlob(data['body'])
    polarity = testimonial.sentiment.polarity 

    if polarity < -0.8 or polarity > 0.8:
        print(testimonial, polarity)

    message.ack()
 
while True:
    #### for every message the subscriber receives it will call the function "callback"
    future = subscriber.subscribe(subscription_name, callback)
    time.sleep(10)
