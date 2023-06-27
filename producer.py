import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import youtube_watcher

# serializing message as JSON
def serializer(message) :
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer (
    bootstrap_servers = ['localhost:9092'],
    value_serializer = serializer
    )


if __name__ == "__main__" :
    # runs continuously until you kill the program
    while True :
        # generate message
        dummy_message = youtube_watcher.main() 


        # send it to out "message" topic
        print(f"Producing message @{datetime.now()} | Message = {str(dummy_message)}")
        producer.send('messages',dummy_message)

        # sleep time
        time_to_sleep = random.randint(1,11)
        time.sleep(time_to_sleep)