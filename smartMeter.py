from google.cloud import pubsub_v1
import json
import time
import random 
import numpy as np
import os

# Read arguments and configurations and initialize
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\carso\Desktop\Milestone3\Design\cred.json"
project_id = "m3-design-380121"
topic_id = "smart_meter_topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


#device normal distributions profile used to generate random data
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091) },
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341) },
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201) },
}
profileNames=["boston","denver","losang"];

# Create a Pub/Sub message
def create_message(record_key, record_value):
    message = {'key': record_key, 'value': record_value}
    message_bytes = json.dumps(message).encode('utf-8')
    return message_bytes

record_key = 0
while True:
    try:
        profile_name = profileNames[random.randint(0, 2)];
        profile = DEVICE_PROFILES[profile_name]
        # get random values within a normal distribution of the value
        temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
        humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
        pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))
        
        # create dictionary
        msg = {"time": time.time(), "profile_name": profile_name, "temperature": temp,"humidity": humd, "pressure":pres};
        
        #randomly eliminate some measurements
        for i in range(3):
            if(random.randrange(0,10)<1):
                choice=random.randrange(0,3)
                if(choice==0):
                    msg['temperature']=None;
                elif (choice==1):
                    msg['humidity']=None;
                else:
                    msg['pressure']=None;
        record_key += 1
        record_value = json.dumps(msg)

        message_bytes = create_message(str(record_key), record_value)
        publisher.publish(topic_path, data=message_bytes)
        
        time.sleep(.5)
    except KeyboardInterrupt:
        break

print("Messages were produced to topic {}!".format(topic_path))
