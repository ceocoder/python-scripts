#!/usr/bin/env python

import simplejson, urllib
import datetime
import sys

class bc:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''

def get_numdocs_size(host):

    result = simplejson.load(urllib.urlopen('http://' + host +':9200/_status'))
    print bc.OKGREEN + '=' * 70  + bc.ENDC
    total_docs = 0
    total_size = 0
    for r in result['indices']:
        docs = result['indices'][r]['docs']['num_docs']
        size = result['indices'][r]['index']['primary_size_in_bytes']
        total_docs += docs
        total_size += size
        print "%r \t %s \t %s" % (r, docs, size)
    print "Total Docs %s for size %s \t %sgb " % (total_docs, total_size,  total_size / 1024 / 1024 / 1024)

def get_node_stats(host):

    print bc.OKGREEN + '=' * 70  + bc.ENDC
    result_node = simplejson.load(urllib.urlopen('http://' + host + ':9200/_cluster/nodes/stats?os=true&jvm=true'))
    now = datetime.datetime.now()
    print bc.OKGREEN + '=' * 70  + bc.ENDC
    print 'Mem @ %s ' % now
    for r in result_node['nodes']:
        a = result_node['nodes'][r]
        print bc.HEADER + 'Mem Node - '         + a['name'] + bc.ENDC
        #print 'Size on Node - ' + a['indices']['store']['size']
        print 'Cache Stats'
        print bc.OKBLUE + '-' * 70  + bc.ENDC
        print 'Field Eviction %s ' % a['indices']['cache']['field_evictions']
        print 'Field Size %s ' % a['indices']['cache']['field_size']
        print bc.OKBLUE + '-' * 70  + bc.ENDC
        print 'Filter Eviction %s ' % a['indices']['cache']['filter_evictions']
        print 'Filter Size %s ' % a['indices']['cache']['filter_size']
        print bc.OKBLUE + '-' * 70  + bc.ENDC
        print 'Mem Stats'
        print 'Mem Used Total %s ' % a['jvm']['mem']['heap_used']
        print 'Mem Committed %s ' % a['jvm']['mem']['heap_committed']
        print bc.OKGREEN + '=' * 70  + bc.ENDC




def print_merge_stats(host):
    result_merge = simplejson.load(urllib.urlopen('http://' + host +':9200/_stats/merge'))
    now = datetime.datetime.now()
    print '--------------- @ %s ' % now
    print 'Total current merges = %s ' % result_merge['_all']['total']['merges']['current']
    print 'Total current size   = %s ' % result_merge['_all']['total']['merges']['current_size']

def get_segment_nums(host):
    result_seg = simplejson.load(urllib.urlopen('http://' + host + ':9200/_segments'))

    for indice in result_seg['indices']:
        ind = result_seg['indices'][indice]
        for shard in ind['shards']: #indice['shards']:
            shrd = result_seg['indices'][indice]['shards'][shard]
            for i in range(2):
                print "%s @ %s %s %s %s" % (indice, shard, shrd[i]['routing']['node'], shrd[i]['num_committed_segments'], shrd[i]['routing']['primary'])


get_numdocs_size(sys.argv[1])
get_node_stats(sys.argv[1])
print_merge_stats(sys.argv[1])
get_segment_nums(sys.argv[1])


