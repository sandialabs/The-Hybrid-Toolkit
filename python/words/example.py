"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
# ===cmd line==
# From the /Hybrid/python/words directory

# In one window
# python words.py

# In another
# python ../executors/hybrid_executor.py --cfg=./words.json



# ==in class usage==


import hybrid
from hybrid import view

json_definitions,classes,aliases=hybrid.utils.loadJSON("words.json")


#q=classes[5]

#v=hybrid.view.create_view_from_query_info(q)

#r = v.rows()
#dblob = r[0]

dbm=classes[8]
ai=classes[6]
aiw=classes[7]
mom=ai.getModelObservationsManager()