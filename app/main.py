#import sys
# update the pip package installer
#!{sys.executable} -m pip install --upgrade --user pip
# install required packages
#!{sys.executable} -m pip install --upgrade --user uproot awkward vector numpy matplotlib


#import uproot # for reading .root files
#import awkward as ak # to represent nested data in columnar format
#import vector # for 4-momentum calculations
#import time # to measure time to analyse
#import math # for mathematical functions such as square root
#import numpy as np # for numerical calculations such as histogramming
#import matplotlib.pyplot as plt # for plotting
#from matplotlib.ticker import AutoMinorLocator # for minor ticks
#import pika

import infofile # local file containing cross-sections, sums of weights, dataset IDs


#lumi = 0.5 # fb-1 # data_A only
#lumi = 1.9 # fb-1 # data_B only
#lumi = 2.9 # fb-1 # data_C only
#lumi = 4.7 # fb-1 # data_D only
lumi = 10 # fb-1 # data_A,data_B,data_C,data_D

fraction = 1.0 # reduce this is if you want the code to run quicker
                                                                                                                                  
#tuple_path = "Input/4lep/" # local 
tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/" # web address

# Define number of workers to be used
number_workers = 4

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
MeV = 0.001
GeV = 1.0

##Get data

def get_data_from_files():

    data = {} # define empty dictionary to hold awkward arrays
    for s in samples: # loop over samples
       # print('Processing '+s+' samples') # print which sample
        frames = [] # define empty list to hold data
        for val in samples[s]['list']: # loop over each file
            if s == 'data': prefix = "Data/" # Data prefix
            else: # MC prefix
                prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."
            fileString = tuple_path+prefix+val+".4lep.root" # file name to open
            temp = read_file(fileString,val) # call the function read_file defined below
            frames.append(temp) # append array returned from read_file to list of awkward arrays
        data[s] = ak.concatenate(frames) # dictionary entry is concatenated awkward arrays
    
    return data # return dictionary of awkward arrays

## Segmenting data
def segment_data(data, number_workers):
    count_events = len(data)
    segment_size = count_events // number_workers
    start_and_end = [ ]
    for i in range(number_workers):
        start = i * segment_size
        end = (i + 1) * segment_size + (count_events % number_workers > i)
        start_and_end.append((start, end))
    return start_and_end


### Need to get the data before defining start and end ####
data = get_data_from_files()

## Get start and end points for each worker
start_and_end = segment_data(data, number_workers)  

## Read file

def read_file(path):
    with uproot.open(path + ":mini") as tree:
       data_all = tree.arrays() # read all data
    
    return data_all



### Data processing

start = time.time() # time at start of whole processing
data = get_data_from_files() # process all files
elapsed = time.time() - start # time after whole processing
print("Time taken: "+str(round(elapsed,1))+"s") # print total time taken to process every file

