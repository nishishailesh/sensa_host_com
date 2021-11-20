#!/usr/bin/python3
import sys
import fcntl

####Settings section start#####
input_tty='/dev/ttyUSB0'
logfile_name='/var/log/sensa.in.log'
output_folder='/root/sensa.inbox.data/' #remember ending/
alarm_time=25
log=1	#log=0 to disable logging; log=1 to enable
####Settings section end#####

import logging
logging.basicConfig(filename=logfile_name,level=logging.DEBUG,format='%(asctime)s %(message)s')
if(log==0):
  logging.disable(logging.CRITICAL)
import signal
import datetime
import time
import serial

def signal_handler(signal, frame):
  global x				#global file open
  global byte_array			#global array of byte
  logging.debug('Alarm stopped')
  sgl='signal:'+str(signal)
  logging.debug(sgl)
  logging.debug(frame)
  try:
    if x!=None:
      x.write(''.join(byte_array))
      x.close()
  except Exception as my_ex:
    logging.debug(my_ex)
  logging.debug(byte_array)
  byte_array=[]							#empty array
  logging.debug('Alarm->signal_handler. data may be incomplate')

def get_filename():
  dt=datetime.datetime.now()
  return output_folder+dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

def get_port():
  port = serial.Serial(input_tty,baudrate=9600)
  return port

def my_read(port):
  return port.read(1)

def my_write(port,byte):
  return port.write(byte)

#main loop##########################

signal.signal(signal.SIGALRM, signal_handler)

port=get_port()
byte_array=[]		#initialized to ensure that first byte can be added
status=''
waiting_for_checksum=False
x=None

start_byte_str=['S','a','m','p','l','e',' ','t','e','s','t',':']             #12 byte long start string
end_byte_str=['\x1b','\x00',' ','\n','\x1b','\x00',' ','\n']
#cur_file=get_filename()                                   #get name of file to open
#logging.debug('opened file:'+cur_file)
#x=open(cur_file,'w')                                        #open file
#fcntl.flock(x, fcntl.LOCK_EX | fcntl.LOCK_NB)   #lock file


while True:
  byte=my_read(port)
  logging.debug(byte)
  if(byte==b''):
    logging.debug('<EOF> reached. disconnected?')
  else:
    byte_array=byte_array+[chr(ord(byte))]	#add everything read to array, if not EOF. EOF have no ord

  if(byte_array[-12:]==start_byte_str):
    logging.debug("Start of Report")
    cur_file=get_filename()                                   #get name of file to open
    logging.debug('opened file:'+cur_file)
    if(x!=None):
      x.close()
      x=None
    x=open(cur_file,'w')                                        #open file
    fcntl.flock(x, fcntl.LOCK_EX | fcntl.LOCK_NB)   #lock file
    byte_array=[]

  if(byte_array[-8:]==end_byte_str and x!=None):		#file opened / start_of_data received
    if(x==None):
      logging.debug("File open problem")
    else:
      logging.debug("File is opened ")

    logging.debug("End of Report Data below:")
    data=''.join(byte_array)
    logging.debug(data)
    x.write(''.join(byte_array))                    #write to file everytime LF received, to prevent big data memory problem
    logging.debug('data written:')
    x.close()
    logging.debug('File was closed:')
    x=None


