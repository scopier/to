## To
- #### Description
#### The repository is a simple, convenient elasticsearch query language for web user, that based on official lib that elasticsearch-dsl and elasticsearch.
- #### Grammar:
```language
The query language grammar:
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
```

- The query language lexer with regex engine and parser with recursive descent algorithm.

- The repository is deploying to my company ops es query platform.

- ` Note: Author not study computer student, so my ability is not very good, so this is for your reference only.`

```
Example:
1.  input  ]# import To; To().parser('status: 500 | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$')
    output ]# {
              "query": {
                "match": {
                  "status": "500"
                }
              },
              "aggs": {
                "group_by_domain": {
                  "terms": {
                    "field": "domain"
                  },
                  "aggs": {
                    "response_time_avg": {
                      "avg": {
                        "field": "response_time"
                      }
                    }
                  }
                }
              }
            }

2.  input  ]# status: 500 | [terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time"), terms("group_by_url", field="url")]$
    output ]# {
              "query": {
                "match": {
                  "status": "500"
                }
              },
              "aggs": {
                "group_by_domain": {
                  "terms": {
                    "field": "domain"
                  },
                  "aggs": {
                    "response_time_avg": {
                      "avg": {
                        "field": "response_time"
                      }
                    }
                  }
                },
                "group_by_url": {
                  "terms": {
                    "field": "url"
                  }
                }
              }
            }

3   input  ]# 'status: 500 and method.keyword ~= GET | terms("group_by_domain", field="domain") | avg("response_time_avg", field="response_time")$'
    output ]# {
              "query": {
                "bool": {
                  "must": [
                    {
                      "match": {
                        "status": "500"
                      }
                    },
                    {
                      "regexp": {
                        "method.keyword": "GET"
                      }
                    }
                  ]
                }
              },
              "aggs": {
                "group_by_domain": {
                  "terms": {
                    "field": "domain"
                  },
                  "aggs": {
                    "response_time_avg": {
                      "avg": {
                        "field": "response_time"
                      }
                    }
                  }
                }
              }
            }
```
