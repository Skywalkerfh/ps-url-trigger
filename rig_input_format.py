#!/usr/bin/env python
# -*- coding: utf-8 -*-

################################################################################
#
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
#
################################################################################

"""
This module provide service.

Author: 
Date:   2017/11/16 11:47:34
"""
import os
import sys
dict = {}
for line in open(sys.argv[1], 'r'):
    fields = line.strip().split('\t')
    name = fields[0]
    pkg = fields[1]
    dict[pkg] = name

for line in open(sys.argv[2], 'r'):
    fields = line.strip().split('\t')
    if len(fields) != 6:
        continue
    query = fields[0]
    title = fields[1]
    url = fields[2]
    pkg = fields[3]
    if pkg not in dict:
        continue
    tag = fields[4]
    rank = fields[5]
    print "%s\t%s\t%s\t%s" % (query, pkg, dict[pkg], '2') 
