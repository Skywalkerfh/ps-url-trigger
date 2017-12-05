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
Date:   2017/06/08 10:18:12
"""
import os
import sys

def certain_pkg_shift(query, pkg):
    term = '微博'.decode('utf8', 'ignore').encode('gbk', 'ignore')
    if pkg == "com.sina.weibo":
        if term not in query:
            return True
    if pkg == "com.youku.phone":
        return True
    return False
        
        
def load_blue_stop_words_dict(dict):
    for line in open('blue_words', 'r'):
        fields = line.strip().split('\t')
        dict[fields[0].decode('utf8', 'ignore').encode('gbk', 'ignore')] = 0
    return dict

def is_blue_word(line, dict):
    blue_words_flag = 0
    for term in dict:
        words = term.strip().split(',')
        if len(words) == 2:
            if words[1] in line and words[0] in line:
                blue_words_flag = 1
                break
        else:
            if words[0] in line:
                blue_words_flag = 1
                break
    if blue_words_flag == 1:
        return True
    return False

def get_ps_title_map():
    blacklist_dict = {}
    for line in open('pkg_showurl_pm_blacklist', 'r'):
        fields = line.strip().split(' ')
        pkg = fields[0]
        url = fields[1]
        if pkg not in blacklist_dict:
            blacklist_dict[pkg] = {}
        blacklist_dict[pkg][url] = 0

    blue_word_dict = {}
    load_blue_stop_words_dict(blue_word_dict)
        
    showurl_dict = {}
    for line in open('pkg_showurl_pm', 'r'):
        fields = line.strip().split('\t')
        pkg = fields[0]
        url = fields[1]
        tag = fields[2]
        if url not in showurl_dict:
            showurl_dict[url] = {}
        showurl_dict[url][pkg] = tag
        
    for line in sys.stdin:
        path = os.environ["map_input_file"]
        fields = line.strip().split('\t')
        if "ps_query_url_click" in path:
            tag = "pc"
            rank = int(fields[5])
        elif "wise_query_url_click" in path:
            tag = "wise"
            rank = int(fields[4])
        query = fields[2]
        url = fields[3]
        title = fields[5]
        if title == '' or int(rank) > 10:
            continue
        if is_blue_word(query, blue_word_dict):
            continue
        for domain in showurl_dict:
            if domain in url:
                for pkg in showurl_dict[domain]:
                    if certain_pkg_shift(query, pkg):
                        continue
                    if pkg in blacklist_dict:
                        flag = 0
                        for url_blacklist in blacklist_dict[pkg]:
                            if url_blacklist in url:
                                flag = 1
                                break
                        if flag == 0:
                            print "%s\t%s\t%s\t%s\t%s\t%s" % (query, title, url, pkg, rank, showurl_dict[domain][pkg])
                    else:
                        print "%s\t%s\t%s\t%s\t%s\t%s" % (query, title, url, pkg, rank, showurl_dict[domain][pkg])
                
def reduce():
    pre_key = ''
    rank_sum = 0
    num = 0
    dict = {}
    pv_dict = {}
    for line in sys.stdin:
        fields = line.strip().split('\t')
        if len(fields) != 6:
            continue
        query = fields[0]
        title = fields[1]
        url = fields[2]
        pkg= fields[3]
        rank = int(fields[4])
        tag = fields[5]
        key = "%s\t%s\t%s" % (query, title, url)
        if pre_key != '' and key != pre_key:
            for inner_pkg in dict:
                rank_avg = float(dict[inner_pkg]['sum_rank'])/dict[inner_pkg]['num']
                if rank_avg > 5.0:
                    continue
                print "%s\t%s" % (dict[inner_pkg]['info'], rank_avg)
            dict = {}
        if pkg not in dict:
            dict[pkg] = {}
            dict[pkg]['sum_rank'] = 0
            dict[pkg]['num'] = 0
            dict[pkg]['info'] = "%s\t%s\t%s\t%s\t%s" % (query, title, url, pkg, tag)
        dict[pkg]['sum_rank'] += rank
        dict[pkg]['num'] += 1
        pre_key = key

if __name__ == '__main__':
    if sys.argv[1] == "get_query_taglist":
        get_query_taglist()
        exit(0)
    elif sys.argv[1] == "map":
        get_ps_title_map()
        exit(0)
    elif sys.argv[1] == "run":
        run()
    elif sys.argv[1] == "red":
        reduce()
        exit(0)
    else:
        print "error"
