import os
import json
import datetime
import pandas as pd
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

payload = {'col1': 'v1', 'col2': 'v2', 'date': str(datetime.datetime.now())}

df:pd.DataFrame = pd.read_gbq(f"""
SELECT * FROM `fh-bigquery.reddit_comments.2006` LIMIT 1000
""", dialect = 'standard'
)
### pubsub needs this in byte-strings
rows = df.to_dict(orient='records')
for idx, row in enumerate(rows):
    payload = row.copy()
    payload['id'] = idx
    payload['data'] = str(datetime.datetime.now())
    publisher.publish(topic_name, json.dumps(payload).encode("utf-8"))

