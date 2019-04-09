# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import json
from to import To

# kibana
# doamin.keyword: www.glodon.com
# domain.keyword: *account.glodon.com
# domain.keyword: www.glodon.com |
# logtail
#


# domain.keyword == '*.glodon.com' | [terms('group_by_method',file="method"),terms(),avg(])
import time
# example 1:
s1 = time.time() * 1000
to = To()
# tsl = 'http_host := "hello world" | [terms("group_by_domain", field="domain")]'
# print to.parser(tsl)
# s2 = time.time() * 1000
# print s2 - s1

#
# to = To()
# tsl = 'status: 600 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
# print json.dumps(to.parser(tsl), indent=2)
# s3 = time.time() * 1000
# print s3 - s2
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
# to = To()
# tsl = 'status: 600 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
# json.dumps(to.parser(tsl), indent=2)
# s6 = time.time() * 1000
# print s6 - s3
# to = To()
tsl = 'status: 500 and method.keyword ~= GET | [terms("group_by_domain", field="domain")| [ terms("group_by_method", field="method"), terms("group_by_only_method", field="method")]]$'
print json.dumps(to.parser(tsl), indent=2)
# s7 = time.time() * 1000
# print s7 - s2
#
# to = To()
# tsl = 'status: 500 or method.keyword ~= GET and domain == [d.glodon.com, a.glodon.com]| terms("group_by_domain", field="domain") | [avg("response_time_avg", field="response_time"), sum("sum_response_size", field="response_size")]$'
# print json.dumps(to.parser(tsl), indent=2)
#
#


# from elasticsearch_dsl.query import Q
# key = "status"
# value = "hello world"
# print Q('match_phrase', **{key: value}).to_dict()


