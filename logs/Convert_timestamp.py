#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 14:43:47 2019

@author: joao
"""

import pandas as pd
from datetime import datetime


base = pd.read_csv("2019-04-05/Base.csv")

base['TIMESTAMP'] = pd.to_datetime(base['TIMESTAMP'], unit='s').dt.tz_localize("GMT").dt.tz_convert('America/Manaus')
print(datetime.utcnow().timestamp())