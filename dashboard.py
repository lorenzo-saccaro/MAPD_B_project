#-------------------- BOKEH DASHBOARD --------------------------

from confluent_kafka import Consumer
import time
import json
import random
import numpy as np
import pandas as pd
from math import floor
from datetime import datetime
from sys import getsizeof
from bokeh.plotting import figure, curdoc
from bokeh.layouts import layout
from bokeh.models import Title, Div, FixedTicker, ColumnDataSource, TableColumn, DataTable

# -------- Connect consumer -----------------------------------

BOOTSTRAP_SERVER = '10.67.22.61'
consumer = Consumer({'bootstrap.servers': BOOTSTRAP_SERVER,
                     'group.id': 0})
consumer.subscribe(['results'])


#-------------------------- Update Function -------------------------------

def update():
    
    msg = consumer.poll(timeout=1)
    while msg == None:
        msg = consumer.poll(timeout=1)
        
    info = json.loads(msg.value())
    
    #-------------------- total hits --------------------
    
    p_line.data_source.data['x'].append(info['msg_ID'])
    p_line.data_source.data['y'].append(info['hit_count'])
    
    p_percent = (max( p_line.data_source.data['y'][-50:]) - min(p_line.data_source.data['y'][-50:])) * 0.05
    p.y_range.start = min(p_line.data_source.data['y'][-50:]) - p_percent
    p.y_range.end   = max( p_line.data_source.data['y'][-50:]) + p_percent
    p_line.data_source.trigger('data', p_line.data_source.data, p_line.data_source.data)
    
    # ----------------- time plot -------------------
    
    now = time.time()
    timestamp.append(now)
    if len(timestamp) > 1:
        timeplot.data_source.data['x'].append(info['msg_ID'])
        timeplot.data_source.data['y'].append(timestamp[-1]-timestamp[-2])
        
        t_percent = (max( timeplot.data_source.data['y'][-20:]) - min(timeplot.data_source.data['y'][-20:])) * 0.05
        t.y_range.start = min(timeplot.data_source.data['y'][-20:]) - t_percent
        t.y_range.end   = max( timeplot.data_source.data['y'][-20:]) + t_percent
        
        timeplot.data_source.trigger('data', timeplot.data_source.data, timeplot.data_source.data)
        
    
    #------------------ info table ---------------------
    
    source.data['Values'][0] = str(p_line.data_source.data['x'][-1] - p_line.data_source.data['x'][0])
    
    N_timestamp = np.array(timestamp)
    elapsed = N_timestamp[-1]-N_timestamp[0]
    hour_tot =floor(elapsed/3600)
    min_tot = floor(elapsed/60 - hour_tot*60)
    sec_tot = floor(elapsed - min_tot*60 - hour_tot*3600)
    source.data['Values'][1] = '{} h {} min {} s'.format(hour_tot,min_tot, sec_tot)
    
    if len(timestamp) >= 2 :
        avg_time = np.mean(N_timestamp[1:] - N_timestamp[:-1])
        std_time = np.std(N_timestamp[1:] - N_timestamp[:-1])
    else:
        avg_time = timestamp[0]
        std_time = 0
    source.data['Values'][2] = '{:6.2f} ± {:6.2f} s'.format(avg_time, std_time)
    
    weight = getsizeof(msg)
    weight_list.append(weight)
    total_weight = sum(weight_list)
    
    if total_weight < 1000:
        source.data['Values'][3] = '{:4.0f} bytes'.format(total_weight)
    elif total_weight > 1000 and total_weight < 1000000:
        source.data['Values'][3] = '{:4.3f} Mb'.format(total_weight/1000)
    else:
        source.data['Values'][3] = '{:4.5f} Gb'.format(total_weight/1000000)
     
    
    source.trigger('data', source.data, source.data)
    
    #------------------ hit per chamber -----------------
    
    l0.data_source.data['x'].append(info['msg_ID'])
    l1.data_source.data['x'].append(info['msg_ID'])
    l2.data_source.data['x'].append(info['msg_ID'])
    l3.data_source.data['x'].append(info['msg_ID'])

    l0.data_source.data['y'].append(info['hit_count_chamber'][1])
    l1.data_source.data['y'].append(info['hit_count_chamber'][2])
    l2.data_source.data['y'].append(info['hit_count_chamber'][3])
    l3.data_source.data['y'].append(info['hit_count_chamber'][4])
    
    q0_percent = (max( l0.data_source.data['y'][-20:]) - min( l0.data_source.data['y'][-20:])) * 0.05
    q1_percent = (max( l1.data_source.data['y'][-20:]) - min( l1.data_source.data['y'][-20:])) * 0.05
    q2_percent = (max( l2.data_source.data['y'][-20:]) - min( l2.data_source.data['y'][-20:])) * 0.05
    q3_percent = (max( l3.data_source.data['y'][-20:]) - min( l3.data_source.data['y'][-20:])) * 0.05
    
    q0.y_range.start = min( l0.data_source.data['y'][-20:]) - q0_percent
    q0.y_range.end   = max( l0.data_source.data['y'][-20:]) + q0_percent
    q1.y_range.start = min( l1.data_source.data['y'][-20:]) - q1_percent
    q1.y_range.end   = max( l1.data_source.data['y'][-20:]) + q1_percent
    q2.y_range.start = min( l2.data_source.data['y'][-20:]) - q2_percent
    q2.y_range.end   = max( l2.data_source.data['y'][-20:]) + q2_percent
    q3.y_range.start = min( l3.data_source.data['y'][-20:]) - q3_percent
    q3.y_range.end   = max( l3.data_source.data['y'][-20:]) + q3_percent

    l0.data_source.trigger('data', l0.data_source.data, l0.data_source.data)
    l1.data_source.trigger('data', l1.data_source.data, l1.data_source.data)
    l2.data_source.trigger('data', l2.data_source.data, l2.data_source.data)
    l3.data_source.trigger('data', l3.data_source.data, l3.data_source.data)

    #----------------- hit per chamber -----------------------
        
    hist0.data_source.data = {'top'  : info['tdc_counts_chamber']['0']['hist_counts'],
                              'left' : info['tdc_counts_chamber']['0']['bin_edges'][:-1],
                              'right': info['tdc_counts_chamber']['0']['bin_edges'][1:]
                             }
    hist1.data_source.data = {'top'  : info['tdc_counts_chamber']['1']['hist_counts'],
                              'left' : info['tdc_counts_chamber']['1']['bin_edges'][:-1],
                              'right': info['tdc_counts_chamber']['1']['bin_edges'][1:]
                             }
    hist2.data_source.data = {'top'  : info['tdc_counts_chamber']['2']['hist_counts'],
                              'left' : info['tdc_counts_chamber']['2']['bin_edges'][:-1],
                              'right': info['tdc_counts_chamber']['2']['bin_edges'][1:]
                             }
    hist3.data_source.data = {'top'  : info['tdc_counts_chamber']['3']['hist_counts'],
                              'left' : info['tdc_counts_chamber']['3']['bin_edges'][:-1],
                              'right': info['tdc_counts_chamber']['3']['bin_edges'][1:]
                             }

    hist0.data_source.trigger('data', hist0.data_source.data, hist0.data_source.data)
    hist1.data_source.trigger('data', hist1.data_source.data, hist1.data_source.data)
    hist2.data_source.trigger('data', hist2.data_source.data, hist2.data_source.data)
    hist3.data_source.trigger('data', hist3.data_source.data, hist3.data_source.data)

    #--------------------------- hit count hist ---------------------------
    
    ghist0.data_source.data = {'top'  : info['active_tdc_chamber']['0']['hist_counts'],
                               'left' : info['active_tdc_chamber']['0']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber']['0']['bin_edges'][1:]
                              }
    ghist1.data_source.data = {'top'  : info['active_tdc_chamber']['1']['hist_counts'],
                               'left' : info['active_tdc_chamber']['1']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber']['1']['bin_edges'][1:]
                              }
    ghist2.data_source.data = {'top'  : info['active_tdc_chamber']['2']['hist_counts'],
                               'left' : info['active_tdc_chamber']['2']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber']['2']['bin_edges'][1:]
                              }
    ghist3.data_source.data = {'top'  : info['active_tdc_chamber']['3']['hist_counts'],
                               'left' : info['active_tdc_chamber']['3']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber']['3']['bin_edges'][1:]
                              }

    ghist0.data_source.trigger('data', ghist0.data_source.data, ghist0.data_source.data)
    ghist1.data_source.trigger('data', ghist1.data_source.data, ghist1.data_source.data)
    ghist2.data_source.trigger('data', ghist2.data_source.data, ghist2.data_source.data)
    ghist3.data_source.trigger('data', ghist3.data_source.data, ghist3.data_source.data)
    
    #--------------------------- hit count \w scint hist ---------------------------
        
    shist0.data_source.data = {'top'  : add_hist_val(shist0.data_source.data['top'], info['active_tdc_chamber_scint']['0']['hist_counts']),
                               'left' : info['active_tdc_chamber_scint']['0']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber_scint']['0']['bin_edges'][1:]
                              }
    shist1.data_source.data = {'top'  : add_hist_val(shist1.data_source.data['top'], info['active_tdc_chamber_scint']['1']['hist_counts']),
                               'left' : info['active_tdc_chamber_scint']['1']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber_scint']['1']['bin_edges'][1:]
                              }
    shist2.data_source.data = {'top'  : add_hist_val(shist2.data_source.data['top'], info['active_tdc_chamber_scint']['2']['hist_counts']),
                               'left' : info['active_tdc_chamber_scint']['2']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber_scint']['2']['bin_edges'][1:]
                              }
    shist3.data_source.data = {'top'  : add_hist_val(shist3.data_source.data['top'], info['active_tdc_chamber_scint']['3']['hist_counts']),
                               'left' : info['active_tdc_chamber_scint']['3']['bin_edges'][:-1],
                               'right': info['active_tdc_chamber_scint']['3']['bin_edges'][1:]
                              }

    shist0.data_source.trigger('data', shist0.data_source.data, shist0.data_source.data)
    shist1.data_source.trigger('data', shist1.data_source.data, shist1.data_source.data)
    shist2.data_source.trigger('data', shist2.data_source.data, shist2.data_source.data)
    shist3.data_source.trigger('data', shist3.data_source.data, shist3.data_source.data)
    
    #--------------------------- driftime hist ---------------------------
    
    dhist0.data_source.data = {'top'  : add_hist_val(dhist0.data_source.data['top'], info['drift_times']['0']['hist_counts']),
                               'left' : info['drift_times']['0']['bin_edges'][:-1],
                               'right': info['drift_times']['0']['bin_edges'][1:]
                              }
    dhist1.data_source.data = {'top'  : add_hist_val(dhist1.data_source.data['top'], info['drift_times']['1']['hist_counts']),
                               'left' : info['drift_times']['1']['bin_edges'][:-1],
                               'right': info['drift_times']['1']['bin_edges'][1:]
                              }
    dhist2.data_source.data = {'top'  : add_hist_val(dhist2.data_source.data['top'], info['drift_times']['2']['hist_counts']),
                               'left' : info['drift_times']['2']['bin_edges'][:-1],
                               'right': info['drift_times']['2']['bin_edges'][1:]
                              }
    dhist3.data_source.data = {'top'  : add_hist_val(dhist3.data_source.data['top'], info['drift_times']['3']['hist_counts']),
                               'left' : info['drift_times']['3']['bin_edges'][:-1],
                               'right': info['drift_times']['3']['bin_edges'][1:]
                              }

    dhist0.data_source.trigger('data', dhist0.data_source.data, dhist0.data_source.data)
    dhist1.data_source.trigger('data', dhist1.data_source.data, dhist1.data_source.data)
    dhist2.data_source.trigger('data', dhist2.data_source.data, dhist2.data_source.data)
    dhist3.data_source.trigger('data', dhist3.data_source.data, dhist3.data_source.data)
    
def add_hist_val(itself , to_sum):
    
    Nitself = np.array(itself)
    Nto_sum = np.array(to_sum)
        
    if len(to_sum)==0:
        pass
    elif len(itself)==0 and len(to_sum)!=0:
        itself = to_sum
    elif len(itself)!=0 and len(to_sum)!=0:
        Nitself = Nitself + Nto_sum
        itself=list(Nitself)
        
    return itself

#-------------------------------------------------------------------------------------------------------------------------------------

# ----------------------- 1. Total Hit plot -------------------------------------------

main_title = Div(text='<h1 style="color:white">_____________________________________<span style="font-family:Helvetica, Arial, sans-serif; color:#404040"> Cosmic Rays Analysis</span></h3>', width=1200, height=60, align='center', sizing_mode="stretch_width")

p = figure(plot_width=700, plot_height=250, title=Title(text="Total number of hits", text_color='#404040'), y_range=(0,100),
           x_axis_label='# Batch', y_axis_label='Number of hits')
p.x_range.follow = "end"
p.x_range.follow_interval = 50
p.x_range.range_padding = 0

p_line = p.line([], [], color="firebrick", line_width=3)

#-------------------------- 2. Time plot -----------------------------

t = figure(plot_width=350, plot_height=250, title=Title(text="Time elapsed since last batch", text_color='#404040'), y_range=(0,100),
          x_axis_label='# Batch', y_axis_label='Time [s]')
t.x_range.follow = "end"
t.x_range.follow_interval = 20
t.x_range.range_padding = 0
timestamp = []

timeplot = t.line([],[],line_width=3)

#-------------------------- 3. Information Table ----------------------

val1='#'; val2='#'; val3='#'; val4='#'; val5='#'
weight_list = []

table_df = pd.DataFrame({
    'Titles': ['Number of recieved batches','Total time elapsed','Average time between batches','Total transmitted Bytes'],
    'Values': [val1,val2,val3,val4]
    })

source = ColumnDataSource(data=table_df)

columns = [
    TableColumn(field='Titles', title='#'),
    TableColumn(field='Values', title='Value'),
    ]

info_table = DataTable(source=source, columns=columns, width=350, height=120,
                       index_position=None, header_row = False, margin=(65,0,65,0) )

#-------------------------- 4. Hit per chamber -------------------------------

q_title = Div(text='<h3 style="color:white">_____________________________________________________________________<span style="font-family: Helvetica, Arial, sans-serif; color:#404040;"> Total numer of hits in each chamber</span></h3>', width=1200, height=45, align='center', sizing_mode="stretch_width")

q0 = figure(plot_width=350, plot_height=250, title="Chamber 0", y_range=(0,100),
            x_axis_label='# Batch', y_axis_label='Number of hits')
q1 = figure(plot_width=350, plot_height=250, title="Chamber 1", y_range=(0,100),
           x_axis_label='# Batch', y_axis_label='Number of hits')
q2 = figure(plot_width=350, plot_height=250, title="Chamber 2", y_range=(0,100),
           x_axis_label='# Batch', y_axis_label='Number of hits')
q3 = figure(plot_width=350, plot_height=250, title="Chamber 3", y_range=(0,100),
           x_axis_label='# Batch', y_axis_label='Number of hits')

q0.x_range.follow = "end"
q0.x_range.follow_interval = 20
q0.x_range.range_padding = 0
q1.x_range.follow = "end"
q1.x_range.follow_interval = 20
q1.x_range.range_padding = 0
q2.x_range.follow = "end"
q2.x_range.follow_interval = 20
q2.x_range.range_padding = 0
q3.x_range.follow = "end"
q3.x_range.follow_interval = 20
q3.x_range.range_padding = 0

l0 = q0.line([], [], color="blue", line_width=3)
l1 = q1.line([], [], color="orange", line_width=3)
l2 = q2.line([], [], color="red", line_width=3)
l3 = q3.line([], [], color="green", line_width=3)


#------------------------------ 5. Hit count hist -----------------------------------

h_title = Div(text='<h3 style="color:white">____________________________________________________________<span style="font-family:Helvetica, Arial, sans-serif; color:#404040"> Histogram of the counts of active TDC_CHANNEL</span></h3>', width=1200, height=45, align='center', sizing_mode="stretch_width")

h0 = figure(width=350, height=250, title = "Chamber 0", x_axis_label='Number of counts')
h1 = figure(width=350, height=250, title = "Chamber 1", x_axis_label='Number of counts')
h2 = figure(width=350, height=250, title = "Chamber 2", x_axis_label='Number of counts')
h3 = figure(width=350, height=250, title = "Chamber 3", x_axis_label='Number of counts')

hist0 = h0.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='blue')
hist1 = h1.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='orange')
hist2 = h2.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='red')
hist3 = h3.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='green')

h_tick = FixedTicker(ticks=[0,10,20,30,40,50,60,70], minor_ticks=[5,15,25,35,45,55,65,75])
h_overrides = {70: '>70'}

h0.xaxis.ticker = h_tick
h0.xaxis.major_label_overrides = h_overrides
h1.xaxis.ticker = h_tick
h1.xaxis.major_label_overrides = h_overrides
h2.xaxis.ticker = h_tick
h2.xaxis.major_label_overrides = h_overrides
h3.xaxis.ticker = h_tick
h3.xaxis.major_label_overrides = h_overrides

#------------------------------ 6. Channel count hist -----------------------------------
g_title = Div(text='<h3 style="color:white">_____________________________________________________<span style="font-family:Helvetica, Arial, sans-serif; color:#404040;"> Histogram of the total number of active TDC_CHANNEL in each ORBIT_CNT</span></h3>', width=1200, height=45, align='center', sizing_mode="stretch_width")

g0 = figure(width=350, height=250, title = "Chamber 0", x_axis_label='Number of channels')
g1 = figure(width=350, height=250, title = "Chamber 1", x_axis_label='Number of channels')
g2 = figure(width=350, height=250, title = "Chamber 2", x_axis_label='Number of channels')
g3 = figure(width=350, height=250, title = "Chamber 3", x_axis_label='Number of channels')

ghist0 = g0.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='blue')
ghist1 = g1.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='orange')
ghist2 = g2.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='red')
ghist3 = g3.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='green')


g_tick = FixedTicker(ticks=[0,5,10,15], minor_ticks=[1,2,3,4,6,7,8,9,11,12,13,14,16])
g_overrides = {15: '>15'}

g0.xaxis.ticker = g_tick
g0.xaxis.major_label_overrides = g_overrides
g1.xaxis.ticker = g_tick
g1.xaxis.major_label_overrides = g_overrides
g2.xaxis.ticker = g_tick
g2.xaxis.major_label_overrides = g_overrides
g3.xaxis.ticker = g_tick
g3.xaxis.major_label_overrides = g_overrides

#------------------------------  7. Channel count \w scint hist -----------------------------------

s_title = Div(text='<h3 style="color:white">_________________________________________<span style="font-family:font-family:Helvetica, Arial, sans-serif; color:#404040;"> Cumulative histogram of the counts of active TDC_CHANNEL, when scintillator register a signal</span></h3>', width=1200, height=45, align='center', sizing_mode="stretch_width")

s0 = figure(width=350, height=250, title = "Chamber 0", x_axis_label='Number of channels')
s1 = figure(width=350, height=250, title = "Chamber 1", x_axis_label='Number of channels')
s2 = figure(width=350, height=250, title = "Chamber 2", x_axis_label='Number of channels')
s3 = figure(width=350, height=250, title = "Chamber 3", x_axis_label='Number of channels')

shist0 = s0.quad(top=[], bottom=0, left=[], right=[], line_alpha=0,  fill_color='blue')
shist1 = s1.quad(top=[], bottom=0, left=[], right=[], line_alpha=0,  fill_color='orange')
shist2 = s2.quad(top=[], bottom=0, left=[], right=[], line_alpha=0,  fill_color='red')
shist3 = s3.quad(top=[], bottom=0, left=[], right=[], line_alpha=0,  fill_color='green')
 
s0.xaxis.ticker = g_tick
s0.xaxis.major_label_overrides = g_overrides
s1.xaxis.ticker = g_tick
s1.xaxis.major_label_overrides = g_overrides
s2.xaxis.ticker = g_tick
s2.xaxis.major_label_overrides = g_overrides
s3.xaxis.ticker = g_tick
s3.xaxis.major_label_overrides = g_overrides

#-------------------------------- 8. Driftime hist ---------------------------------

d_title = Div(text='<h3 style="color:white">____________________________________________________________________<span style="font-family:Helvetica, Arial, sans-serif; color:#404040;"> Cumulative histogram of the driftime</span></h3>', width=1200, height=45, align='center', sizing_mode="stretch_width")

d0 = figure(width=350, height=250, title = "Chamber 0", x_axis_label='Time [ns]')
d1 = figure(width=350, height=250, title = "Chamber 1", x_axis_label='Time [ns]')
d2 = figure(width=350, height=250, title = "Chamber 2", x_axis_label='Time [ns]')
d3 = figure(width=350, height=250, title = "Chamber 3", x_axis_label='Time [ns]')

dhist0 = d0.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='blue')
dhist1 = d1.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='orange')
dhist2 = d2.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='red')
dhist3 = d3.quad(top=[], bottom=0, left=[], right=[], line_alpha=0, fill_color='green')

d_tick = FixedTicker(ticks=[30,150,270,390,510], minor_ticks=[0,60,90,120,180,210,240,300,330,360,420,450,480,540])
d_overrides = {510: '>510'}

d0.xaxis.ticker = d_tick
d0.xaxis.major_label_overrides = d_overrides
d1.xaxis.ticker = d_tick
d1.xaxis.major_label_overrides = d_overrides
d2.xaxis.ticker = d_tick
d2.xaxis.major_label_overrides = d_overrides
d3.xaxis.ticker = d_tick
d3.xaxis.major_label_overrides = d_overrides

#---------------------------- final things --------------------------------

lay_out = layout([ [main_title],
                   [p,t, info_table],
                   [q_title],
                   [q0, q1, q2, q3],
                   [h_title],
                   [h0, h1, h2, h3],
                   [g_title],
                   [g0, g1, g2, g3],
                   [s_title],
                   [s0, s1, s2, s3],
                   [d_title],
                   [d0, d1, d2, d3],
                 ])
curdoc().add_root(lay_out)
curdoc().theme = 'light_minimal'
curdoc().add_periodic_callback(update, 1)

