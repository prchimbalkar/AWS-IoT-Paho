""" Send File Using MQTT """
import time
import paho.mqtt.client as paho
import hashlib
import ssl

MQTT_PORT = 8883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "file-transfer"
MQTT_QOS = 1
DATA_BLOCK_SIZE=128000

MQTT_HOST = "a1eql2alc32utf-ats.iot.us-west-2.amazonaws.com"
CA_ROOT_CERT_FILE = "root-CA.crt"
THING_CERT_FILE = "FirstThing.cert.pem"
THING_PRIVATE_KEY = "FirstThing.private.key"
FILE_NAME="test-out.zip"


fout=open(FILE_NAME,"wb") #use a different FILE_NAME
# for outfile as I'm running sender and receiver together

def process_message(msg):
   """ This is the main receiver code
   """
   print("received ")
   global bytes_in
   if len(msg)==200: #is header or end
      print("found header")
      msg_in=msg.decode("utf-8")
      msg_in=msg_in.split(",,")
      if msg_in[0]=="end": #is it really last packet?
         in_hash_final=in_hash_md5.hexdigest()
         if in_hash_final==msg_in[2]:           
            print("File copied OK -valid hash  ",in_hash_final)
            return -1
         else:
            print("Bad file receive   ",in_hash_final)
         return False
      else:
         if msg_in[0]!="header":
            in_hash_md5.update(msg)
            return True
         else:
            return False
   else:
      bytes_in=bytes_in+len(msg)
      in_hash_md5.update(msg)
      print("found data bytes= ",bytes_in)
      return True
#define callback
def on_message(client, userdata, message):
   #time.sleep(1)
   global run_flag
   #print("received message =",str(message.payload.decode("utf-8")))
   ret=process_message(message.payload)
   if ret:
      fout.write(message.payload)
   if ret== -1:
      run_flag=False #exit receive loop
      print("complete file received")
   


bytes_in=0
client= paho.Client("client-receive-001")  #create client object client1.on_publish = on_publish                          #assign function to callback client1.connect(broker,port)                                 #establish connection client1.publish("data/files","on")  
client.tls_set(CA_ROOT_CERT_FILE, certfile=THING_CERT_FILE, keyfile=THING_PRIVATE_KEY, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

######
client.on_message=on_message
client.mid_value=None
#####
print("connecting to broker ")
client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)#connect
#client.loop_start() #start loop to process received messages
print("subscribing ")
client.subscribe(MQTT_TOPIC)#subscribe
time.sleep(2)
start=time.time()
time_taken=time.time()-start
in_hash_md5 = hashlib.md5()
run_flag=True
while run_flag:
   client.loop(00.1)  #manual loop
   pass
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
print(current_time)
client.disconnect() #disconnect
#client.loop_stop() #stop loop
fout.close()

