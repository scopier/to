# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import re
import json
from threading import RLock
from elasticsearch_dsl import Q as ESQ
from elasticsearch_dsl import A as ESA
from elasticsearch import Elasticsearch


__all__ = ['QuerySyntaxError', 'To']


class QuerySyntaxError(Exception):

    def __str__(self):
        return 'query syntax error'


class LRUPool(object):

    def __init__(self, size):
        self.size = size
        self.cache = {}
        self.lock = RLock()
        self.ordered = []

    def add(self, k, value):
        """

        :param k:
        :param value:
        :return:
        """
        with self.lock:
            if len(self.cache) > self.size:
                old = self.ordered.pop()
                del self.cache[old]
            self.ordered.append(k)
            self.cache[k] = value

    def get(self, k):
        """

        :param k:
        :return:
        """
        with self.lock:
            if k in self.cache:
                self.ordered.remove(k)
                self.ordered.append(k)
                return self.cache[k]


class LRUCache(object):
    """
        cache hot query str then pop last recently used query str avoid parsing again
    """

    def __init__(self, mode, size=1024):
        """

        :param mode: cache groups
        :param size: cache key size
        """
        self.mode = mode
        self.cache = {i: LRUPool(size) for i in range(mode)}

    def get(self, key):
        """

        :param key:
        :return:
        """
        k = id(key)
        m = k % self.mode
        print m
        return self.cache[m].get(key)

    def add(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        k = id(key)
        m = k % self.mode
        print m
        self.cache[m].add(k, value)

    def __call__(self, parser):
        """

        :param parser:
        :return:
        """

        def wrap(obj, expression):
            tsl = self.get(expression)
            if not tsl:
                tsl = parser(obj, expression)
                self.add(expression, tsl)
            return tsl

        return wrap


class Regex(object):
    """
        query or filter abstract class
    """
    regex = None
    name = None
    expr = None

    def __init__(self):
        self.value = None

    @classmethod
    def match(cls, s, pos):
        """

        :param pos:
        :param s:
        :return:
        """
        m = re.compile(cls.regex).match(s, pos)
        if m:
            token = cls()
            token.value = m.group()
            return m.end(), token
        return None


class Query(Regex):

    @property
    def instance(self):
        params = self.value.split(self.expr)
        key = str(params[0]).strip()
        value = str(params[1]).strip()
        return ESQ(self.name, **{key: value})


class Match(Query):
    """
        match full text search
    """
    name = 'match'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_\.-]*\s*:\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    expr = ':'


class MatchPhrase(Query):
    """
        match pharse full text search
    """
    name = 'match_phrase'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*:=\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    expr = ':='


class Term(Query):
    """
        term search
    """
    name = 'term'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*===\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    expr = '==='


class Terms(Query):
    """
        terms search
    """
    name = 'terms'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.\-]*\s*==\s*\[[a-zA-Z0-9]+[a-zA-Z0-9_.\-]*(\s*,\s*[a-zA-Z0-9]+[a-zA-Z0-9_.\-]*)*\]'

    @property
    def instance(self):
        params = self.value.split('==')
        key = str(params[0]).strip()
        value = map(lambda x: x.strip(), str(params[1]).strip('[]').split(','))
        return ESQ(self.name, **{key: value})


class Regexp(Query):
    """
        regexp search
    """
    name = 'regexp'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*~=\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    expr = '~='


class Wildcard(Query):
    """
        wildcard search
    """
    name = 'wildcard'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*@=\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    expr = '@='


class Prefix(Query):
    """
        prefix search
    """
    name = 'prefix'
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*#=\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    expr = '#='


class RangeRegex(Query):
    """
        range regex
    """
    symbol = None

    @property
    def instance(self):
        params = self.value.split(self.expr)
        key = str(params[0]).strip()
        value = str(params[1]).strip()
        return ESQ(self.name, **{key: {self.symbol, value}})


class LtRange(RangeRegex):
    """
        range search for lt
    """
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*<\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    symbol = 'lt'
    expr = '<'


class LteRange(RangeRegex):
    """
        range search for lte
    """
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*<=\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    symbol = 'lte'
    expr = '<='


class GtRange(RangeRegex):
    """
        range search for gt
    """
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*>\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    symbol = 'gt'
    expr = '>'


class GteRange(RangeRegex):
    """
        range search for gte
    """
    regex = r'[a-zA-Z0-9]+[a-zA-Z0-9_.-]*\s*>=\s*[a-zA-Z0-9]+[a-zA-Z0-9_.-]*'
    symbol = 'gte'
    expr = '>='


class A(object):

    def __init__(self, agg):
        """

        :param agg:
        """
        self.agg = agg
        self.name = None
        self.statement = None
        self.root = None
        self.children = []

    def __call__(self, name, field, **kwargs):
        """

        :param field:  agg field
        :param kwargs:  agg params
        """
        self.name = name
        self.statement = ESA(self.agg, field=field, **kwargs).to_dict()
        return self

    def to_dict(self):
        """
            return aggregation chain expression
        :return:
        """
        current, root = self, self
        while root:
            agg = {}
            children = root.children
            for child in children:
                agg[child.name] = child.statement
            if agg:
                root.statement['aggs'] = agg
            current = root
            root = current.root
        return {current.name: current.statement}

    def __or__(self, other):
        """
            build aggregation chain
        :param other:
        :return:
        """
        if isinstance(other, list):
            self.children.extend(other)
            return self
        else:
            self.children.append(other)
            other.root = self
            return other

    def __repr__(self):
        return json.dumps(self.to_dict())


class Bucket(Regex):
    """
        Bucket aggregation abstract class
    """

    @property
    def instance(self):
        return eval(self.value, {'__builtins__': {self.name: A(self.name)}})


class TermsBucket(Bucket):
    regex = r'terms\(.*?\)'
    name = 'terms'


class Range(Bucket):
    regex = r'range\(.*?\)'
    name = 'range'


class IPRange(Bucket):
    regex = r'ip_range\(.*?\)'
    name = 'ip_range'


class DateRange(Bucket):
    regex = r'date_range\(.*?\)'
    name = 'date_range'


class Histogram(Bucket):
    regex = r'histogram\(.*?\)'
    name = 'histogram'


class DateHistogram(Bucket):
    regex = r'date_histogram\(.*?\)'
    name = 'date_histogram'


class Metric(Regex):
    """
        Metric aggregation abstract class
    """

    @property
    def instance(self):
        return eval(self.value, {'__builtins__': {self.name: A(self.name)}})


class Cardinality(Metric):
    regex = r'cardinality\(.*?\)'
    name = 'cardinality'


class Avg(Metric):
    regex = r'avg\(.*?\)'
    name = 'avg'


class Max(Metric):
    regex = r'max\(.*?\)'
    name = 'max'


class Min(Metric):
    regex = r'min\(.*?\)'
    name = 'min'


class Sum(Metric):
    regex = r'sum\(.*?\)'
    name = 'sum'


class Count(Metric):
    regex = r'count\(.*?\)'

    @property
    def instance(self):
        return eval(self.value, {'__builtins__': {'count': A('value_count')}})


class Stats(Metric):
    regex = r'stats\(.*?\)'
    name = 'stats'


class Percentiles(Metric):
    regex = r'percentiles\(.*?\)'
    name = 'percentiles'


class LParenth(Regex):
    regex = r'\('

    @property
    def instance(self):
        return self.value


class RParenth(Regex):
    regex = r'\)'

    @property
    def instance(self):
        return self.value


class LBracket(Regex):
    regex = r'\['

    @property
    def instance(self):
        return self.value


class RBracket(Regex):
    regex = r'\]'

    @property
    def instance(self):
        return self.value


class LcAnd(Regex):
    regex = r'\band\b'

    @property
    def instance(self):
        return self.value


class LcOr(Regex):
    regex = r'\bor\b'

    @property
    def instance(self):
        return self.value


class LcNot(Regex):
    regex = r'\bnot\b'

    @property
    def instance(self):
        return self.value


class Comma(Regex):
    regex = r','

    @property
    def instance(self):
        return self.value


class EOF(Regex):
    regex = r'\$'

    @property
    def instance(self):
        return self.value


class AP(Regex):
    """
        aggregation pipe symbol
    """
    regex = r'\|'

    @property
    def instance(self):
        return self.value


class WhiteSpace(Regex):
    regex = r'(?:\s|\t)+'  # 空格符号

    @property
    def instance(self):
        return self.value


class To(object):
    """
        es sketchy language grammar:
            S -> Q '|' A
            S -> Q
            S -> A

        Q is query dsl language grammar:
            Q -> T Q'
            Q' -> or T Q' | empty
            T -> F T'
            T' -> and F T' | empty
            F -> (Q)
            F -> ~Q
            F -> query

        A is aggregation language grammar:
            A  -> B '|' Metric
            A  -> Metric

            B  -> C B'
            B' -> '|' C B'
            C  -> [ D ]
            C  -> Bucket

            D  -> A A'
            A' -> ,A A' | empty

    """
    REGEXS = [
        WhiteSpace, Comma, EOF, AP,  # 空格 逗号 等字符
        LcAnd, LcOr, LcNot,  # 复合查询
        Match, MatchPhrase, Term, Terms,  # 短语查询
        Wildcard, Prefix, Regexp,  # 模糊查询
        GtRange, LtRange, LteRange, GteRange,  # 范围查询
        TermsBucket, Histogram, DateHistogram, Range, IPRange, DateRange,  # 分组
        Avg, Max, Min, Sum, Count, Stats, Cardinality,  # 聚合
        LParenth, RParenth, LBracket, RBracket,  # 圆括号 方括号
    ]
    LruMode = 5
    lru = LRUCache(LruMode)

    def __init__(self):
        """
            sketchy es query expression
        """
        self.pos = 0
        self.tokens = []

    def lexer(self, expression):
        """
            use regexp split expression to tokens
        :return:
        """
        length, pos, i = len(expression), 0, 0
        while pos < length:
            i += 1
            for regex in self.REGEXS:
                match = regex.match(expression, pos)
                if match:
                    pos, token = match
                    if not isinstance(token, WhiteSpace):
                        self.tokens.append(token)
                    break
            if i > length:
                raise QuerySyntaxError()

    def query(self):
        """
            query function (Q) entry
        :return:
        """
        query = self.queryLcAnd(self.queryExpr())
        if query:
            query = self.queryLcOr(query)
        return query

    def queryExpr(self):
        """
            query function (F) entry
        :return:
        """
        token = self.tokens[self.pos]
        if isinstance(token, LcNot):
            self.pos += 1
            return ~ self.query()
        elif isinstance(token, LParenth):
            self.pos += 1
            query = self.query()
            self.pos += 1  # 删掉多余的')'
            return query
        elif isinstance(token, Query):
            self.pos += 1
            return token.instance
        else:
            return

    def queryLcOr(self, q):
        """
            query function (Q') entry
        :param q:
        :return:
        """
        token = self.tokens[self.pos]
        if not isinstance(token, LcOr):
            return q
        else:
            self.pos += 1
            q1 = self.queryLcAnd(self.queryExpr())
            q = q | q1
            return self.queryLcOr(q)

    def queryLcAnd(self, q):
        """
            query function (T') entry
        :param q:
        :return:
        """
        token = self.tokens[self.pos]
        if not isinstance(token, LcAnd):
            return q
        else:
            self.pos += 1
            q1 = self.queryExpr()
            q = q & q1
            return self.queryLcAnd(q)

    def aggs(self):
        """
            aggregation function (A) entry
        :return:
        """
        token = self.tokens[self.pos]
        if isinstance(token, Metric):
            self.pos += 1
            if not isinstance(self.tokens[self.pos], (EOF, RBracket, Comma)):
                raise QuerySyntaxError()
            return token.instance
        if isinstance(token, Bucket):
            self.pos += 1
            return self.bucket(token.instance)
        if isinstance(token, LBracket):
            self.pos += 1
            aggs = self.subAgg(self.aggs())
            return aggs

    def bucket(self, bucket=None):
        """
            aggregation function (Bucket)
        :param bucket:
        :return:
        """
        if isinstance(self.tokens[self.pos], AP):
            self.pos += 1
            sub = self.aggs()
            bucket |= sub
            return self.bucket(bucket)
        return bucket

    def subAgg(self, agg):
        """
            aggregation function (D)
        :param agg:
        :return:
        """
        token = self.tokens[self.pos]
        if isinstance(token, Comma):
            self.pos += 1
            agg = [agg, self.aggs()]
            return self.subAgg(agg)
        if isinstance(token, RBracket):
            self.pos += 1
            return agg
        return agg

    @lru
    def parser(self, expression):
        """
            es language parse entry
        :return:
        """
        ret = {'query': {}, 'aggs': {}}
        # 1. 词法分析
        self.lexer(expression)
        # 2. 查询语法
        query = self.query()
        if query and isinstance(self.tokens[self.pos], AP):
            self.pos += 1
        # 3. 聚合语法
        aggs = self.aggs()
        if not isinstance(self.tokens[self.pos], EOF):
            raise QuerySyntaxError()
        if query:
            ret['query'] = query.to_dict()
        if isinstance(aggs, list):
            for agg in aggs:
                ret['aggs'].update(agg.to_dict())
        if isinstance(aggs, A):
            ret['aggs'].update(aggs.to_dict())
        return ret

    def search(self, hosts, index, query):
        """
            init elasticsearch client
        :param hosts:
        :param index:
        :param query:
        :return:
        """
        dsl = self.parser(query)
        client = Elasticsearch(hosts=hosts)
        return client.search(index, body=dsl)

