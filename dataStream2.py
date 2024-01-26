


import requests, json
from kafka import KafkaProducer
import time
from bson import json_util


#list of topic names
topic_names = ["utilizations", "utilizations01", "utilizations03"]

#create producer
producer = KafkaProducer(bootstrap_servers="129.114.26.151:9092")
print("connecting to Kafka topic...")


#apikey for api call
api_key = "97b4a3a7a7ebd51c123556e03913fe9c"

#base url for api call
base_url = "https://api.openweathermap.org/data/2.5/weather?"

#Da Nang, Bay Area, Nashville
city_add = [("16.0545","108.0717"), ("37.8272", "122.2913"), ("40.7128, 74.0060")]

#TODO: Mi: change first index (of city_add) to 0, Blake: change first index to 1, Daniel: change first index to 2
city_lat = city_add[2][0]
city_long = city_add[2][1]

complete_url = base_url + "lat=" + city_lat + "&lon=" + city_long + "&appid=" + api_key


# Run 5 times for each producer to get total of 15
for i in range(0,5):
    response = requests.get(complete_url)
    resp_json = response.json()
    print(resp_json)
    res = json.dumps(resp_json, default = json_util.default).encode('utf-8')    
    #TODO: Same as above
    producer.send(topic=topic_names[2], value = res)
    producer.flush()

    time.sleep(1)

producer.close()





