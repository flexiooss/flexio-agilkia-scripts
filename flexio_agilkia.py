#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: aahmad
"""

import pandas as pd
import agilkia
import sys

if len(sys.argv) < 3:
    print("usage : Agilkia_flexio.py inputfile outputfile")
    exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

logs = pd.read_csv(input_file)
#logs = pd.concat([logs, pd.read_csv("cleaned_data_week47.csv")])

logs.sort_values(by=['execution_id','date'])
logs['action'] = logs.resource_action
logs.loc[logs.scope == "TRIGGER", 'action'] = logs.loc[logs.scope == "TRIGGER", 'ressource_trigger']
logs.loc[logs.scope == "RECORD", 'action'] = "RECORD"
logs.drop(['event','connector','resource_action','ressource_trigger'],axis=1, inplace=True)

actions_char = agilkia.default_map_to_chars(logs.action.unique())

#%%
def session_generator(logs):
    session_logs = logs
    i = (session_logs[1:].scope.values == 'TRIGGER').argmax() + 1
    yield session_logs[:i]
    while(True):
        session_logs = session_logs[i:]
        
        if not (session_logs[1:].scope.values == 'TRIGGER').size :
            return
        
        i = (session_logs[1:].scope.values == 'TRIGGER').argmax() + 1
        
        
        yield session_logs[:i]
        
#%%    
def create_traces(logs):
    traceset = agilkia.TraceSet([])
    for sess in session_generator(logs):
        trace = agilkia.Trace([])
        for index, row in sess.iterrows():
            inputs = {}
            outputs = {}
            others = {
                    'timestamp': row.date,
                    'sessionID': row.execution_id,
                    'process_id': row.process_id,
                    'object': row.scope,
                    }
            event = agilkia.Event(row.action, inputs, outputs, others)
            trace.append(event)
        traceset.append(trace)
    return traceset

#%%
    
traceset_bis = create_traces(logs)    
traceset_bis.save_to_json(output_file)