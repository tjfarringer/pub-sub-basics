import os
import json
from google.cloud import pubsub_v1
from dotenv import load_dotenv
load_dotenv()

import warnings 
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    topic=os.getenv('TOPIC_NAME'),  # Set this to something appropriate.
)

payload = {'col1': 'v1', 'col2': 'v2'}
### pubsub needs this in byte-strings
publisher.publish(topic_name, json.dumps(payload).encode("utf-8"))

