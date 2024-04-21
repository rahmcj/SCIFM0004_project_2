### Might need to have it so all the data is added together here
### but also might be on main.py
import pika
import uproot
import awkward as ak
import vector
import time
import json
import infofile
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator  # for minor ticks

# Define the merged data structure
merged_data = {}

### Units ###
MeV = 0.001
GeV = 1.0

# Connect to RabbitMQ
def rabbitmq_connect(host, retries=10, delay=5):
    for i in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError:
            print(f'Failed to connect to {host}, retrying in {delay} seconds')
            time.sleep(delay)
    raise Exception(f'Failed to connect to {host} after {retries} retries')

# Function to process each segment of data
def process_segment(segment):
    sample = segment['sample']
    data = segment['data']

    if sample not in merged_data:
        merged_data[sample] = data
    else:
        merged_data[sample] = ak.concatenate([merged_data[sample], data])

# Callback function for receiving messages
def callback(ch, method, properties, body):
    segment = json.loads(body)
    process_segment(segment)
    print(f'Processed {segment["sample"]} segment')

# Connect to RabbitMQ
connection = rabbitmq_connect('rabbitmq')
channel = connection.channel()  
channel.queue_declare(queue='segmented_data')   
channel.basic_consume(queue='segmented_data', on_message_callback=callback, auto_ack=True)

# Wait for messages
print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# Plot the merged data
def plot_data(merged_data):
    xmin = 80 * GeV
    xmax = 250 * GeV
    step_size = 5 * GeV

    bin_edges = np.arange(start=xmin, # The interval includes this value
                                            stop=xmax+step_size, # The interval doesn't include this value
                                            step=step_size ) # Spacing between values
    bin_centres = np.arange(start=xmin+step_size/2, # The interval includes this value
                                            stop=xmax+step_size/2, # The interval doesn't include this value
                                            step=step_size ) # Spacing between values

    data_x,_ = np.histogram(ak.to_numpy(merged_data['data']['mllll']), 
                                                 bins=bin_edges ) # histogram the data
    data_x_errors = np.sqrt( data_x ) # statistical error on the data

    signal_x = ak.to_numpy(merged_data[r'Signal ($m_H$ = 125 GeV)']['mllll']) # histogram the signal
    signal_weights = ak.to_numpy(merged_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

    mc_x = [] # define list to hold the Monte Carlo histogram entries
    mc_weights = [] # define list to hold the Monte Carlo weights
    mc_colors = [] # define list to hold the colors of the Monte Carlo bars
    mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

    for s in samples: # loop over samples
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
            mc_x.append( ak.to_numpy(merged_data[s]['mllll']) ) # append to the list of Monte Carlo histogram entries
            mc_weights.append( ak.to_numpy(merged_data[s].totalWeight) ) # append to the list of Monte Carlo weights
            mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
            mc_labels.append( s ) # append to the list of Monte Carlo legend labels
    


    # *************
    # Main plot 
    # *************
    main_axes = plt.gca() # get current axes
    
    # plot the data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                                        fmt='ko', # 'k' means black and 'o' is for circles 
                                        label='Data') 
    
    # plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges, 
                                weights=mc_weights, stacked=True, 
                                color=mc_colors, label=mc_labels )
    
    mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value
    
    # calculate MC statistical uncertainty: sqrt(sum w^2)
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    
    # plot the signal bar
    main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, 
                                 weights=signal_weights, color=signal_color,
                                 label=r'Signal ($m_H$ = 125 GeV)')
    
    # plot the statistical uncertainty
    main_axes.bar(bin_centres, # x
                                2*mc_x_err, # heights
                                alpha=0.5, # half transparency
                                bottom=mc_x_tot-mc_x_err, color='none', 
                                 hatch="////", width=step_size, label='Stat. Unc.' )

    # set the x-limit of the main axes
    main_axes.set_xlim( left=xmin, right=xmax ) 
    
    # separation of x axis minor ticks
    main_axes.xaxis.set_minor_locator( AutoMinorLocator() ) 
    
    # set the axis tick parameters for the main axes
    main_axes.tick_params(which='both', # ticks on both x and y axes
                                                 direction='in', # Put ticks inside and outside the axes
                                                 top=True, # draw ticks on the top axis
                                                  right=True ) # draw ticks on right axis
    
    # x-axis label
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                                             fontsize=13, x=1, horizontalalignment='right' )
    
    # write y-axis label for main axes
    main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                                             y=1, horizontalalignment='right') 
    
    # set y-axis limits for main axes
    main_axes.set_ylim( bottom=0, top=np.amax(data_x)*1.6 )
    
    # add minor ticks on y-axis for main axes
    main_axes.yaxis.set_minor_locator( AutoMinorLocator() ) 

    # Add text 'ATLAS Open Data' on plot
    plt.text(0.05, # x
                0.93, # y
                'ATLAS Open Data', # text
                transform=main_axes.transAxes, # coordinate system used is that of main_axes
                fontsize=13 ) 
    
    # Add text 'for education' on plot
    plt.text(0.05, # x
             0.88, # y
             'for education', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             style='italic',
             fontsize=8 ) 
    
    # Add energy and luminosity
    lumi_used = str(lumi*fraction) # luminosity to write on the plot
    plt.text(0.05, # x
             0.82, # y
             r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes
    
    # Add a label for the analysis carried out
    plt.text(0.05, # x
             0.76, # y
             r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text 
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes

    # draw the legend
    main_axes.legend( frameon=False ) # no box around the legend
    
    pass

# Call the function to plot the data
plot_data(merged_data)
plt.show()

