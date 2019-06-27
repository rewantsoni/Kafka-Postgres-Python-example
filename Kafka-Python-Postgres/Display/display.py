from json import loads,dumps
from flask import Flask, request, render_template, url_for, redirect
from kafka import KafkaConsumer, KafkaProducer
import json
from time import sleep

app = Flask(__name__)


@app.route('/')
def index():
    print('producer')
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: 
    dumps(x).encode('utf-8'))
    s={'username' : 'all'}
    producer.send('sampleTest',value=json.dumps(s))
    print('produced')
    sleep(1)
    print('going for consumer')
    while(True):
        print('comsumer')
        consumer = KafkaConsumer(
        'sampleTestReply',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group2',
         max_poll_interval_ms=10,
         value_deserializer=lambda x: loads(x.decode('utf-8')))
        print(consumer)
        print(consumer)
        for msg in consumer:
            print(msg)
            msg=msg.value
            try:
                json_object = json.loads(msg)
            except:
                print('error in json')
            print(json_object)
            print(json_object[1]['id'])
            return(render_template('index.html',users=json_object))

def checkconsumer():
    print('hey')
if __name__ == '__main__':
    app.run()