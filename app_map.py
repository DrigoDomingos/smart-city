from flask import Flask, render_template
import datetime
import json
from time import sleep
from json import dumps
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError
from flask import request
from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map



app = Flask(__name__)

#Global Variables
ELK_SERVER_NAME = ""
BOOTSTRAP_SERVER = '192.168.1.106:9092'
KAFKA_SERVER_NAME = "192.168.1.106:2181"
TOPIC = 'alerts'

p = Producer({'bootstrap.servers': BOOTSTRAP_SERVER})

c =  Consumer({'bootstrap.servers': BOOTSTRAP_SERVER
	             ,'group.id': 'mygroup6'
				 ,'auto.offset.reset':'earliest'})


app.config['GOOGLEMAPS_KEY'] = "AIzaSyAdAQWRgzuUksc-Pywh43ywtj61cexUA_8"
# Initialize the extension
GoogleMaps(app)




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
		
		return json_data
		
	def SendToKafka(self,p):			

		
		def delivery_report(err, msg):
			""" Called once for each message produced to indicate delivery result.
				Triggered by poll() or flush(). """
			if err is not None:
				print('Message delivery failed: {}'.format(err))
			else:
				print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
		
						
			
		some_data_source = self.BuildJson()


			# Trigger any available delivery report callbacks from previous produce() calls
		p.poll(0)

			# Asynchronously produce a message, the delivery report callback
			# will be triggered from poll() above, or flush() below, when the message has
			# been successfully delivered or failed permanently.
		p.produce(TOPIC, some_data_source.encode('utf-8'), callback=delivery_report)

		# Wait for any outstanding messages to be delivered and delivery report
		# callbacks to be triggered.
		p.flush()		

@app.route('/')
def start():
    # creating a map in the view
   
	fullmap = Map(
        identifier="fullmap",
		lat=37.4419,
        lng=-122.1419,        
		style="height:500px;width:90%;margin:0;",
		cluster=True,
		cluster_imagepath='https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m',
		center_on_user_location=True,
		
        markers=[
          {
             'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
             'lat': 37.4419,
             'lng': -122.1419,
             'infobox': "<b>Hello World</b>"
          },
          {
             'icon': 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
             'lat': 37.4300,
             'lng': -122.1400,
             'infobox': "<b>Hello World from other place</b>"
          }
        ]
    )

	'''	
	c.subscribe([TOPIC])

	while True:
		msg = c.poll(1.0)

		if msg is None:
			continue
		if msg.error():
			print("Consumer error: {}".format(msg.error()))
			continue

		print('Received message: {}'.format(msg.value().decode('utf-8')))

	c.close()
		
	'''	
	return render_template('index.html', fullmap=fullmap,list='teste')

   

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
				
				message.SendToKafka(p)
				
				del message
				
				return render_template('finish.html')
				
		except Exception as e:
			return 'Error loading the file, please try again'
	else:
		return 'teste'
   

if __name__ == '__main__':
   app.run(debug = True)