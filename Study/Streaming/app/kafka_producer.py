from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))
i = 0

while True:
    message = {"text": "Hello World! " + str(i)}
    producer.send('test-topic', value=message)
    print("Sent message: " + json.dumps(message))
    i += 1
    time.sleep(5)
