from flask import Flask, render_template
import datetime
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError
from flask import request


app = Flask(__name__)

#Global Variables
ELK_SERVER_NAME = ""
BOOTSTRAP_SERVER = '192.168.56.3:9092'
KAFKA_SERVER_NAME = "192.168.56.3:2181"
TOPIC = 'alerts'

producer_kfk = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),acks="all")
						 




class SCMessage:
	
	def __init__(self,EMAIL,ITEM_TYPE,DETAILS,LATITUDE,LONGITUDE,DATETIME):
		self.email = EMAIL
		self.type = ITEM_TYPE
		self.details = DETAILS
		self.latitude = LATITUDE
		self.longitude = LONGITUDE
		self.datetime = DATETIME
		
	def BuildJson(self):
		#Convert object into JSON 
		
		
		data = {}
		data['email'] = self.email
		data['item_type'] = self.type
		data['details'] = self.details
		data['latitude'] = self.latitude
		data['longitude'] = self.longitude
		data['datetime'] = self.datetime		
		json_data = json.dumps(data)
		
		return data
		
	def SendToKafka(self,producer_kfk):	
				
		for _ in range(1):							
			data = self.BuildJson()	
			producer_kfk.send(TOPIC, value=data)
			producer_kfk.flush()
			#producer_kfk.pool()		

@app.route('/')
def start():

   
   return render_template('index.html')
   
@app.route('/form_request')
def form_request():
   return render_template('request.html')
   
@app.route('/producer',  methods = ['GET', 'POST'])
def producer():
	if request.method == 'POST':
		try:
			if request.method == "POST":
			
							
				message = SCMessage(request.form.get('email'),
									request.form.get('type'),
									request.form.get('details'),
									str(request.form.get('lat')),
									str(request.form.get('long')),
									str(datetime.datetime.now())
				                   )	
				
				message.SendToKafka(producer_kfk)
				
				del message
				
				return render_template('finish.html')
				
		except Exception as e:
			return 'Error loading the file, please try again'
	else:
		return 'teste'
   

if __name__ == '__main__':
   app.run(debug = True)