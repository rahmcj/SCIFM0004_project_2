import pika
import uproot
import awkward as ak
import vector
import time
import json
import infofile
import numpy as np
import zlib

### Units ###
MeV = 0.001
GeV = 1.0
# url for data
tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/" # web address
#lumi = 0.5 # fb-1 # data_A only
#lumi = 1.9 # fb-1 # data_B only
#lumi = 2.9 # fb-1 # data_C only
#lumi = 4.7 # fb-1 # data_D only
lumi = 10 # fb-1 # data_A,data_B,data_C,data_D
fraction = 1.0 # reduce this is if you want the code to run quicker


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

# Connect to RabbitMQ
def rabbitmq_connect(host, retries=10, delay=5):
    for i in range (retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError:
            print(f'Failed to connect to {host}, retrying in {delay} seconds')
            time.sleep(delay)
    raise Exception(f'Failed to connect to {host} after {retries} retries')


### Calculate weight of MC event
def calc_weight(xsec_weight, events):
    return (
        xsec_weight
        * events.mcWeight
        * events.scaleFactor_PILEUP
        * events.scaleFactor_ELE
        * events.scaleFactor_MUON 
        * events.scaleFactor_LepTRIGGER
    )



### Get cross section of weight

def get_xsec_weight(sample):
    info = infofile.infos[sample] # open infofile
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    return xsec_weight # return cross-section weight


### Calculate 4-lepton invariate mass
def calc_mllll(lep_pt, lep_eta, lep_phi, lep_E):
    # construct awkward 4-vector array
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    # calculate invariant mass of first 4 leptons
    # [:, i] selects the i-th lepton in each event
    # .M calculates the invariant mass
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV

# cut on lepton charge
# paper: "selecting two pairs of isolated leptons, each of which is comprised of two leptons with the same flavour and opposite charge"
def cut_lep_charge(lep_charge):
# throw away when sum of lepton charges is not equal to 0
# first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

# cut on lepton type
# paper: "selecting two pairs of isolated leptons, each of which is comprised of two leptons with the same flavour and opposite charge"
def cut_lep_type(lep_type):
# for an electron lep_type is 11
# for a muon lep_type is 13
# throw away when none of eeee, mumumumu, eemumu
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

### Function to process data
def process_segment(field_list, tuple_path):
    field = field_list.decode('utf-8')
    pref = field.split()[0]
    val = field.split()[1]
    sample = val
    fileString = tuple_path+pref+val+".4lep.root" # file name to open
    
    data_all = [] # empty list to hold data

    start = time.time() # start timer
    # open the tree called mini using a context manager (will automatically close files/resources)
    with uproot.open(fileString + ":mini") as tree:
        numevents = tree.num_entries # number of events
        if 'data' not in sample: xsec_weight = get_xsec_weight(sample) # get cross-section weight
        for data in tree.iterate(['lep_pt','lep_eta','lep_phi',
                                  'lep_E','lep_charge','lep_type', 
                                  # add more variables here if you make cuts on them 
                                  'mcWeight','scaleFactor_PILEUP',
                                  'scaleFactor_ELE','scaleFactor_MUON',
                                  'scaleFactor_LepTRIGGER'], # variables to calculate Monte Carlo weight
                                 library="ak", # choose output type as awkward array
                                 entry_stop=numevents*fraction): # process up to numevents*fraction
            if 'data' not in sample: # only do this for Monte Carlo simulation files
                # multiply all Monte Carlo weights and scale factors together to give total weight
                data['totalWeight'] = calc_weight(xsec_weight, data)

            # cut on lepton charge using the function cut_lep_charge defined above
            data = data[~cut_lep_charge(data.lep_charge)]

            # cut on lepton type using the function cut_lep_type defined above
            data = data[~cut_lep_type(data.lep_type)]

            # calculation of 4-lepton invariant mass using the function calc_mllll defined above
            data['mllll'] = calc_mllll(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_E)

            # array contents can be printed at any stage like this
            #print(data)

            # array column can be printed at any stage like this
            #print(data['lep_pt'])

            # multiple array columns can be printed at any stage like this
            #print(data[['lep_pt','lep_eta']])

        
            data_all.append(data) # append array from this batch
            total_time = time.time() - start # calculate total time taken
    
    data = ak.concatenate(data_all) # concatenate all of the arrays
    ser_data = ak.to_list(data) # serialize the awkward array
    data_and_sample = { 'sample': sample, 'data': ser_data } # create dictionary with sample name and data
    pre_zipped_data = json.dumps(data_and_sample) # convert dictionary to JSON
    zipped_data = zlib.compress(pre_zipped_data.encode('utf-8')) # compress serialised data
    print(total_time) # print total time taken for comparison
    return zipped_data

def callback(ch, method, properties, body):
    try:
        # process data
        data = process_segment(body, tuple_path)

        #send to output processor
        ch.basic_publish(exchange='', routing_key='processed_data', body=data)
        print(f"Processed data sent to outputter")
    except Exception as e:
        print(f"Failed to process data: {e}")
#    segment = json.loads(body)

 #   if isinstance(segment, list):
  #      segment = segment[0]

   # if isinstance(segment, dict):
    #    sample = segment.get('sample')
     #   data = segment.get('data')

      #  if data is not None:
       #     processed_data = process_segment(data)
        #    return processed_data   

# connect to rabbitMQ
connection = rabbitmq_connect('rabbitmq')
channel = connection.channel()  
# declare queue to receive messages from producer
channel.queue_declare(queue='segmented_data')   
# declare queue to send messages to outputter
channel.queue_declare(queue='processed_data')

# enbale consumer to receive messages from segmented_data queue
channel.basic_consume(queue='segmented_data', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
