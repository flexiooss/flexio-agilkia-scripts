#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: aahmad
"""

import pandas as pd
import sys

if len(sys.argv) < 3:
    print("usage : flexio_prep.py inputfile outputfile")
    exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]
print("Input  :", input_file)
print("Output :", output_file)

logs = pd.read_json(input_file, lines=True)

cleaned_logs = logs[logs["data"].notnull()]
cleaned_logs = cleaned_logs[cleaned_logs["count"] > 0]

# %%
cleaned_logs["execution_id"] = cleaned_logs.data.apply(lambda row: row.get("executionId", ""))
cleaned_logs = cleaned_logs[cleaned_logs["execution_id"].notnull()]
cleaned_logs = cleaned_logs[cleaned_logs["execution_id"] != ""]
cleaned_logs = cleaned_logs[cleaned_logs["scope"] != "RECORD"]
# %%
cleaned_logs.drop(['id', 'logReadingId'], axis=1, inplace=True)

# %%
cleaned_logs["process_id"] = cleaned_logs.data.apply(lambda row: row.get("processId", ""))
cleaned_logs["connector"] = cleaned_logs.data.apply(lambda row: row.get("connector", ""))
cleaned_logs["resource_action"] = cleaned_logs.data.apply(lambda row: row.get("resourceAction", ""))
cleaned_logs["resource"] = cleaned_logs.data.apply(lambda row: row.get("ressource", ""))
cleaned_logs["ressource_trigger"] = cleaned_logs.data.apply(lambda row: row.get("resourceTrigger", ""))
# %%

cleaned_logs.drop(['data', 'extension', 'resource', 'user', 'count'], axis=1, inplace=True)
cleaned_logs.replace(to_replace=["EveryHourTrigger", "EveryDayAtTrigger", "EveryWeekAtTrigger"],
                     value="DateTrigger",
                     inplace=True)
# %%

cleaned_logs.to_csv(output_file)
print("Done")
