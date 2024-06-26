import uproot # for reading .root files
import awkward as ak # to represent nested data in columnar format
import time # to measure time to analyse
import pika # for rabbitMQ
import infofile # local file containing cross-sections, sums of weights, dataset IDs
import json
import os

lumi = 10 # fb-1 # data_A,data_B,data_C,data_D
fraction = 1.0 # reduce this is if you want the code to run quicker                                                                                                                         
#tuple_path = "Input/4lep/" # local 
tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/" # web address
# Define number of workers to be used
number_workers = 2


    


samples = {

    'data': {
        'list' : ['data_A','data_B','data_C','data_D'],
    },

    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" # purple
    },

    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" # red
    },

    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff" # light blue
    },

}


### Units ###
#MeV = 0.001
#GeV = 1.0

# Get RabbitMQ hostname from environment variable
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')

# Define rabbitMQ connection
def rabbitmq_connection(host, retries=10, delay=5):
    for i in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError:
            print(f"Failed to connect to {host}, retrying in {delay} seconds")
            time.sleep(delay)
    raise Exception(f"Failed to connect to {host} after {retries} retries")

##Get data

def get_data_from_files(samples):

    fields = [] # define empty list to hold awkward arrays
    for s in samples: # loop over samples
        for val in samples[s]['list']: # loop over each file
            if s == 'data': 
                prefix = "Data/" # Data prefix
            else: # MC prefix
                prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."

            field=prefix+' '+val
            fields.append(field) # append file name to list of fields
           # fileString = tuple_path+prefix+val+".4lep.root" # file name to open
            #temp = read_file(fileString,val) # call the function read_file defined below
            #frames.append(temp) # append array returned from read_file to list of awkward arrays
        #data[s] = ak.concatenate(frames) # dictionary entry is concatenated awkward arrays
    return fields # return list of fields to send to consumers

field_list = get_data_from_files(samples)


## Segmenting data
#def segment_data(data, number_workers):
 #   count_events = len(data)
  #  segment_size = count_events // number_workers
   # start_and_end = [ ]
    #for i in range(number_workers):
     #   start = i * segment_size
      #  end = (i + 1) * segment_size + (count_events % number_workers > i)
       # start_and_end.append((start, end))
    #return start_and_end




## Read file

#def read_file(path, val):
 #   try:
  #      with uproot.open(path + ":mini") as tree:
   #         data_all = tree.arrays() # read all data
    #    return data_all
    #except Exception as e:
     #   print(f"Failed to read file {path}")
      #  return None

connection = rabbitmq_connection(rabbitmq_host) 

def send_data_to_consumers(field_list):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='segmented_data')
    for segment in field_list:
        channel.basic_publish(exchange='', routing_key='segmented_data', body=segment)
    
        print(f"Sent {segment}")# send each segment to consumers
    print("All data sent")
    connection.close()
    #return

send_data_to_consumers(field_list) 

#def process_data():
 #   data = get_data_from_files()
    #start_and_end = segment_data(data, number_workers)
    #send_data_to_workers(start_and_end)

#data = process_data()
#print(data)

#start = time.time() # time at start of whole processing
#data = get_data_from_files() # process all files
#elapsed = time.time() - start # time after whole processing
#print("Time taken: "+str(round(elapsed,1))+"s") # print total time taken to process every file

