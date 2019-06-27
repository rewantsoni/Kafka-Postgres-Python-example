from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from sqlalchemy import Column, Integer, String, Date
from kafka import KafkaConsumer, KafkaProducer
import json
from json import loads,dumps
from time import sleep

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres+psycopg2://username:password@localhost:5432/testdb'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)
manager = Manager(app)
manager.add_command('db', MigrateCommand)

class User(db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String(80), unique=True)
    email = db.Column(db.String(120), unique=True)

    def __init__(self, name, email):
        self.name = name
        self.email = email
    ##TO DISPLAY SEPIFICALLY
    # def __repr__(self):
    #     # return '["{}", "{}","{}"]'.format(self.id,self.name,self.email)
    #     return (self.id,self.name,self.email)  

@app.route('/')
def index():
    return 'Hello!'
def returnall():
    users=User.query.all()
    print(users)
    key=['id','name','email']
    s=[]
    d={}
    for user in users:
        for column in user.__table__.columns:
            d[column.name] = str(getattr(user, column.name))
        print(d)
        s.append(d.copy())
        print("s is {}".format(s))
        print(type(d))
        print(type(s))
    print(s)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: 
    dumps(x).encode('utf-8'))
    # s={'hey' : 10}
    producer.send('sampleTestReply',value=json.dumps(s))
    sleep(1)
    print('done')
# if __name__ == "__main__":
#     app.run()
def check():
    consumer = KafkaConsumer(
    'sampleTest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     # max_poll_interval_ms=10,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
    # consumer=[{'username':'all'}]
    for msg in consumer:
        msg = json.loads(msg.value)
        print(msg['username'])
        if msg['username'] == 'all':
            returnall()
        else:
            # returnuser()
            print('nothing')