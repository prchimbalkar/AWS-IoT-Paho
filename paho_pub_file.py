import paho.mqtt.client as paho
import ssl
import time
import hashlib

# Define Variables
MQTT_PORT = 8883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "file-transfer"
MQTT_QOS = 1
DATA_BLOCK_SIZE=128000

MQTT_HOST = "a1eql2alc32utf-ats.iot.us-west-2.amazonaws.com"
CA_ROOT_CERT_FILE = "root-CA.crt"
THING_CERT_FILE = "FirstThing.cert.pem"
THING_PRIVATE_KEY = "FirstThing.private.key"
FILE_NAME="test-in.zip"

fo=open(FILE_NAME,"rb")
# file_out="out-"+FILE_NAME
# fout=open(file_out,"wb") #use a different FILE_NAME
# # for outfile as I'm running sender and receiver together

def on_publish(client, userdata, mid):
    #logging.debug("pub ack "+ str(mid))
    client.mid_value=mid
    client.puback_flag=True  

## waitfor loop
def wait_for(client,msgType,period=0.25,wait_time=40,running_loop=False):
    client.running_loop=running_loop #if using external loop
    wcount=0
    #return True
    while True:
        #print("waiting"+ msgType)
        if msgType=="PUBACK":
            if client.on_publish:        
                if client.puback_flag:
                    return True
     
        if not client.running_loop:
            client.loop(.01)  #check for messages manually
        time.sleep(period)
        #print("loop flag ",client.running_loop)
        wcount+=1
        if wcount>wait_time:
            print("return from wait loop taken too long")
            return False
    return True 
def send_header(FILE_NAME):
   header="header"+",,"+FILE_NAME+",,"
   header=bytearray(header,"utf-8")
   header.extend(b','*(200-len(header)))
   print(header)
   c_publish(client,MQTT_TOPIC,header,MQTT_QOS)
def send_end(FILE_NAME):
   end="end"+",,"+FILE_NAME+",,"+out_hash_md5.hexdigest()
   end=bytearray(end,"utf-8")
   end.extend(b','*(200-len(end)))
   print(end)
   c_publish(client,MQTT_TOPIC,end,MQTT_QOS)
def c_publish(client,MQTT_TOPIC,out_message,MQTT_QOS):
   res,mid=client.publish(MQTT_TOPIC,out_message,MQTT_QOS)#publish
   #return

   if res==0: #published ok
      if wait_for(client,"PUBACK",running_loop=True):
         if mid==client.mid_value:
            print("match mid ",str(mid))
            client.puback_flag=False #reset flag
         else:
            print("quitting")
            raise SystemExit("not got correct puback mid so quitting")
         
      else:
         raise SystemExit("not got puback so quitting")

client= paho.Client("client-001")  #create client object client1.on_publish = on_publish                          #assign function to callback client1.connect(broker,port)                                 #establish connection client1.publish("data/files","on")  
client.tls_set(CA_ROOT_CERT_FILE, certfile=THING_CERT_FILE, keyfile=THING_PRIVATE_KEY, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
######
#client.on_message=on_message
client.on_publish=on_publish
client.puback_flag=False #use flag in publish ack
client.mid_value=None
#####
print("connecting to broker ")
client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)#connect
client.loop_start() #start loop to process received messages
print("subscribing ")
#client.subscribe(MQTT_TOPIC)#subscribe
#time.sleep(2)
start=time.time()
print("publishing ")
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
print(current_time)
send_header(FILE_NAME)
Run_flag=True
count=0
out_hash_md5 = hashlib.md5()
in_hash_md5 = hashlib.md5()
bytes_out=0

while Run_flag:
   chunk=fo.read(DATA_BLOCK_SIZE)
   if chunk:
      out_hash_md5.update(chunk) #update hash
      out_message=chunk
      #print(" length =",type(out_message))
      bytes_out=bytes_out+len(out_message)

      c_publish(client,MQTT_TOPIC,out_message,MQTT_QOS)
   else:
      #end of file so send hash
      out_message=out_hash_md5.hexdigest()
      send_end(FILE_NAME)
      Run_flag=False
time_taken=time.time()-start
print("took ",time_taken)
print("bytes sent =",bytes_out)
time.sleep(10)
client.disconnect() #disconnect
client.loop_stop() #stop loop
fo.close()