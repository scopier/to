# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import json
from to import To


import time
# example 1:
s1 = time.time() * 1000
to = To()
tsl = 'status: 500 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
json.dumps(to.parser(tsl), indent=2)
s2 = time.time() * 1000
print s2 - s1
to = To()
tsl = 'status: 600 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
json.dumps(to.parser(tsl), indent=2)
s3 = time.time() * 1000
print s3 - s2
# to = To()
# tsl = 'status: 500 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
# json.dumps(to.parser(tsl), indent=2)
# s4 = time.time() * 1000
# print s4 - s3
# to = To()
# tsl = 'status: 600 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
# json.dumps(to.parser(tsl), indent=2)
# s5 = time.time() * 1000
# print s5 - s4
to = To()
tsl = 'status: 600 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
json.dumps(to.parser(tsl), indent=2)
s6 = time.time() * 1000
print s6 - s3
to = To()
tsl = 'status: 500 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
json.dumps(to.parser(tsl), indent=2)
s7 = time.time() * 1000
print s7 - s2
# example 2:
# to = To()
# tsl = 'status: 500 or method.keyword ~= GET and domain == [d.glodon.com, a.glodon.com]| terms("group_by_domain", field="domain") | [avg("response_time_avg", field="response_time"), sum("sum_response_size", field="response_size")]$'
# print json.dumps(to.parser(tsl), indent=2)



