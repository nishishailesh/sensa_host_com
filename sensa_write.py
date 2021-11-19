#!/usr/bin/python3
import sys
import fcntl
import logging
import time
import io
from astm_bidirectional_common import my_sql , file_mgmt, print_to_log
#For mysql password
sys.path.append('/var/gmcs_config')
import astm_var
####Settings section start#####
logfile_name='/var/log/sensa.out.log'
inbox_data='/root/sensa.inbox.data/' #remember ending/
inbox_arch='/root/sensa.inbox.arch/' #remember ending/
log=1	#log=0 to disable logging; log=1 to enable
equipment='SENSA'

####Settings section end#####

logging.basicConfig(filename=logfile_name,level=logging.DEBUG,format='%(asctime)s %(message)s')
if(log==0):
  logging.disable(logging.DEBUG)
  
print_to_log("Logging Test","[OK]")

f=file_mgmt()
f.set_inbox(inbox_data,inbox_arch)
print_to_log("Inbox Data at:",f.inbox_data)
print_to_log("Inbox Archived at:",f.inbox_arch)
record_start_string=b"Sample test:"		#12 byte long start string
record_end_string=b'\x1b\x00 \n'  		#4 byte long end string, required twice consecutively to end record


def get_eid_for_sid_code(ms,con,sid,ex_code,equipment):
  logging.debug(sid)
  prepared_sql='select examination_id from result where sample_id=%s'
  data_tpl=(sid,)
  logging.debug(prepared_sql)
  logging.debug(data_tpl)

  cur=ms.run_query(con,prepared_sql,data_tpl)
  
  eid_tpl=()
  data=ms.get_single_row(cur)
  while data:
    logging.debug(data)
    eid_tpl=eid_tpl+(data[0],)
    data=ms.get_single_row(cur)
  logging.debug(eid_tpl)
  

  prepared_sqlc='select examination_id from host_code where code=%s and equipment=%s'
  data_tplc=(ex_code,equipment)
  logging.debug(prepared_sqlc)
  logging.debug(data_tplc)
  curc=ms.run_query(con,prepared_sqlc,data_tplc)
  
  eid_tplc=()
  datac=ms.get_single_row(curc)
  while datac:
    logging.debug(datac)
    eid_tplc=eid_tplc+(datac[0],)
    datac=ms.get_single_row(curc)
  logging.debug(eid_tplc)

  ex_id=tuple(set(eid_tpl) & set(eid_tplc))
  logging.debug('final examination id:'+str(ex_id))
  if(len(ex_id)!=1):
    msg="Number of examination_id found is {}. only 1 is acceptable.".format(len(ex_id))
    logging.debug(msg)
    return False
  return ex_id[0]


def get_eid_for_sid_code_blob(ms,con,sid,ex_code,equipment):
  logging.debug(sid)
  prepared_sql='select examination_id from result_blob where sample_id=%s'
  data_tpl=(sid,)
  logging.debug(prepared_sql)
  logging.debug(data_tpl)

  cur=ms.run_query(con,prepared_sql,data_tpl)
  
  eid_tpl=()
  data=ms.get_single_row(cur)
  while data:
    logging.debug(data)
    eid_tpl=eid_tpl+(data[0],)
    data=ms.get_single_row(cur)
  logging.debug(eid_tpl)
  

  prepared_sqlc='select examination_id from host_code where code=%s and equipment=%s'
  data_tplc=(ex_code,equipment)
  logging.debug(prepared_sqlc)
  logging.debug(data_tplc)
  curc=ms.run_query(con,prepared_sqlc,data_tplc)
  
  eid_tplc=()
  datac=ms.get_single_row(curc)
  while datac:
    logging.debug(datac)
    eid_tplc=eid_tplc+(datac[0],)
    datac=ms.get_single_row(curc)
  logging.debug(eid_tplc)

  ex_id=tuple(set(eid_tpl) & set(eid_tplc))
  logging.debug('final examination id:'+str(ex_id))
  if(len(ex_id)!=1):
    msg="Number of examination_id found is {}. only 1 is acceptable.".format(len(ex_id))
    logging.debug(msg)
    return False
  return ex_id[0]

def analyse_file(fh):
  all_records_dict={}
  one_record_dict={}
  one_line=''
  record_end_status=0;
  sample_id=None
  while True:
      one_line=fh.readline()
      print_to_log("one_line:",one_line)
      
      #update dictionary
      line_list=one_line.split(b'\x1b\x00')
      print_to_log("line_list:",line_list)
      line_list_len=len(line_list)
      print_to_log("line_list_len:",line_list_len)

      if(line_list_len==2):
        one_record_dict.update({  one_line.split(b'\x1b\x00')[1].strip():''})
        sub_split=one_line.split(b'\x1b\x00')[1].strip().split(b':')
        print_to_log("#sub-split by :",sub_split)
        if(sub_split[0].strip()==b'Patient ID'):
          sample_id=sub_split[1].strip()
          print_to_log("#Sample ID",sample_id)
                  
      if(line_list_len>=3):
        one_record_dict.update({one_line.split(b'\x1b\x00')[1].strip():one_line.split(b'\x1b\x00')[2].strip()})

        
      #take dicisions          
      if one_line==b'':
        break
      elif (one_line[:12]==record_start_string):
        print_to_log(">>>>>>>It starts here>>>>>>>",one_line)
        one_record_dict={}										#empty dictionary
        record_end_status=0										#reset record end status
        print_to_log("#Record End Status#",record_end_status)
      elif (one_line[:4]==record_end_string):
        print_to_log(">>>>>>>Record End String Found>>>>>>>",one_line)
        record_end_status=record_end_status+1;
        print_to_log("#Record End Status#",record_end_status)
      
      if(record_end_status==2):
        all_records_dict.update({sample_id:one_record_dict})
        one_record_dict={}										#empty dictionary
        record_end_status=0										#reset record end status
        sample_id=None
                        
  return all_records_dict
  
def manage_all_record(all_record_dict):
  for record in all_record_dict:
    #print(record)
    manage_record(record,all_record_dict[record])

def manage_record(sid,data):
  print_to_log("sample_id:",sid)
  print_to_log("data:",data)

  date_time=data[b'Date'][6:]
  print_to_log("date_time:",date_time)

  pH=data[b'pH'].split()[0]
  print_to_log("pH:",pH)

  pCO2=data[b'pCO2'].split()[0]
  print_to_log("pCO2:",pCO2)

  pO2=data[b'pO2'].split()[0]
  print_to_log("pO2:",pO2)

  Na=data[b'Na'].split()[0]
  print_to_log("Na:",Na)

  K=data[b'K'].split()[0]
  print_to_log("K:",K)

  iCa=data[b'iCa'].split()[0]
  print_to_log("iCa:",iCa)

  Cl=data[b'Cl'].split()[0]
  print_to_log("Cl:",Cl)

  GLU=data[b'GLU'].split()[0]
  print_to_log("GLU:",GLU)

  LAC=data[b'LAC'].split()[0]
  print_to_log("LAC:",LAC)

  HCO3=data[b'HCO3'].split()[0]
  print_to_log("HCO3:",HCO3)

  ms=my_sql()
  con=ms.get_link(astm_var.my_host,astm_var.my_user,astm_var.my_pass,astm_var.my_db)


  prepared_sql='insert into sensa \
                             (date_time,sample_id,pH,pCO2,pO2,Na,K,iCa,Cl,GLU,LAC,HCO3) \
                             values \
                             (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
  data_tpl=(date_time,sid,pH,pCO2,pO2,Na,K,iCa,Cl,GLU,LAC,HCO3)
  try:
    cur=ms.run_query(con,prepared_sql,data_tpl)
    msg=prepared_sql
    print_to_log('prepared_sql:',msg)
    msg=data_tpl
    print_to_log('data tuple:',msg)
    #print_to_log('cursor:',cur)
    ms.close_cursor(cur)

  except Exception as my_ex:
    msg=prepared_sql
    print_to_log('prepared_sql:',msg)
    msg=data_tpl
    print_to_log('data tuple:',msg)
    print_to_log('exception description:',my_ex)

while True:
  if(f.get_first_inbox_file()):
    all_record_tuple=analyse_file(f.fh)
    print_to_log("all record:",all_record_tuple)
    manage_all_record(all_record_tuple)
    f.archive_inbox_file()
    time.sleep(1)
