import pika
import uproot
import awkward as ak
import vector
import time
import json
import infofile
import numpy as np

### Units ###
MeV = 0.001
GeV = 1.0

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
def process_segment(segment):
    sample = segment['sample']
    data = segment['data']

    if sample is not None and 'data' not in sample:
        xsec_weight = get_xsec_weight(sample)  # get cross-section weight
        data['totalWeight'] = calc_weight(xsec_weight, data)  # multiply all Monte Carlo weights and scale factors together to give total weight

    # cut on lepton charge using the function cut_lep_charge defined above
    data = data[~cut_lep_charge(data.lep_charge)]

    # cut on lepton type using the function cut_lep_type defined above
    data = data[~cut_lep_type(data.lep_type)]

    # calculation of 4-lepton invariant mass using the function calc_mllll defined above
    data['mllll'] = calc_mllll(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_E)

    return data

def callback(ch, method, properties, body):
    segment = json.loads(body)
    processed_segment = process_segment(segment)
    print(f'Processed {len(processed_segment)} events')

# connect to rabbitMQ
connection = rabbitmq_connect('localhost')
channel = connection.channel()  
channel.queue_declare(queue='segmented_data')   

# enbale consumer to receive messages from segmented_data queue
channel.basic_consume(queue='segmented_data', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()