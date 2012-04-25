#!/usr/bin/env python

import simplejson, urllib
import datetime
import sys

def get_numdocs_size(host):
    
    result = simplejson.load(urllib.urlopen('http://' + host +':9200/_status'))
    print '==========================================================================='
    total_docs = 0
    total_size = 0
    for r in result['indices']:
        docs = result['indices'][r]['docs']['num_docs']
        size = result['indices'][r]['index']['primary_size_in_bytes']
        total_docs += docs
        total_size += size
        print r + '\t' +  str(docs) + '\t' + str(size)
    print "Total Docs " + str(total_docs) + " for size " + str(total_size) + '\t' + str(total_size / 1024 / 1024 / 1024) + 'gb'

def get_node_stats(host):
   
    print '================================================================'
    result_node = simplejson.load(urllib.urlopen('http://' + host + ':9200/_cluster/nodes/stats'))
    now = datetime.datetime.now()
    print '------------------------------------------'
    print 'Mem @ ' + str(now)
    for r in result_node['nodes']:
        a = result_node['nodes'][r]
        print 'Mem Node - '         + a['name']
        #print 'Size on Node - ' + a['indices']['store']['size']
        print 'Cache Stats'
        print '----------------------------------------------------------------'
        print 'Field Eviction ' + str(a['indices']['cache']['field_evictions'])
        print 'Field Size '     + a['indices']['cache']['field_size']
        print '----------------------------------------------------------------'
        print 'Filter Eviction ' + str(a['indices']['cache']['filter_evictions'])
        print 'Filter Size '     + a['indices']['cache']['filter_size']
        print '----------------------------------------------------------------'
        print 'Mem Stats'
        print 'Mem Used Total ' + a['jvm']['mem']['heap_used']
        print 'Mem Committed ' + a['jvm']['mem']['heap_committed']
        print '================================================================'
    
 


def print_merge_stats(host):
    result_merge = simplejson.load(urllib.urlopen('http://' + host +':9200/_stats/merge'))
    now = datetime.datetime.now()
    print '--------------- @ ' + str(now)
    print 'Total current merges = ' + str(result_merge['_all']['total']['merges']['current'])
    print 'Total current size   = ' + str(result_merge['_all']['total']['merges']['current_size'])

def get_segment_nums(host):
    result_seg = simplejson.load(urllib.urlopen('http://' + host + ':9200/_segments'))
    
    for indice in result_seg['indices']:
        ind = result_seg['indices'][indice]
        for shard in ind['shards']: #indice['shards']:
            shrd = result_seg['indices'][indice]['shards'][shard]
            if shrd[0]['num_committed_segments'] > 1:
                print indice + ' @ ' + shard + ' ' + shrd[0]['routing']['node']  + ' ' + str(shrd[0]['num_committed_segments'])


get_numdocs_size(sys.argv[1])
get_node_stats(sys.argv[1])
print_merge_stats(sys.argv[1])
get_segment_nums(sys.argv[1])
        

