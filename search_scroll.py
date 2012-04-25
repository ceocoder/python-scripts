#!/usr/bin/env python

import simplejson, urllib
import sys

def run_scroll_search(host, index, doc_type, field, query):
    url = 'http://{0}:9200/{1}/{2}/_search?q={3}:{4}'.format(host, index, doc_type, field, query)
    result = simplejson.load(urllib.urlopen(url))
    print 'Got {0} results in {1}ms'.format(str(result['hits']['total']), str(result['took']))
    for hit in result['hits']['hits']:
        print hit['_id'] + ' ' + hit['_source']['title']

if len(sys.argv) == 6:
    run_scroll_search(int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
else:
    print 'Enter args like <host> <index_name> <doc_type> <field> <query>'
  
