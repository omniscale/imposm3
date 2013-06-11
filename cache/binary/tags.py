# query taginfo for most used

import json
import urllib2

URL = "http://taginfo.openstreetmap.org/api/4/tags/popular?sortname=count_%s&sortorder=desc&page=1&rp=200&qtype=tag"

added = set()

def codepoints_for(elem_type, min_count=100000):
    resp = urllib2.urlopen(URL % elem_type)
    data = json.load(resp)
    by_count = []
    for item in data['data']:
        if item['in_wiki'] != 1:
            continue
        if item['key'] not in ('source', 'source_ref', 'attribution', 'import', 'import_uuid'):
            by_count.append((item['count_%s' % elem_type], item['key'], item['value'].encode('utf8'), item['count_%s_fraction' % elem_type]))

    by_count.sort(reverse=True)
    fraction = 0.0
    for item in by_count:
        fraction += item[-1]
        if item[0] < min_count:
            break
        key_val = (item[1], item[2])
        if key_val in added:
            print '//',
        added.add(key_val)
        print 'addTagCodePoint("%s", "%s")' % key_val


if __name__ == '__main__':
    print '// most used tags for ways'
    codepoints_for('ways')
    print '// most used tags for nodes'
    codepoints_for('nodes')
    print '// most used tags for rels'
    codepoints_for('relations')