# 时序RDF及时序查询

### 生成时序RDF数据集`tLUBM`

#### 生成`LUBM` ID三元组数据集
见 [Preparing RDF datasets](INSTALL.md#preparing-rdf-datasets)

#### 为三元组随机生成有效时间区间

```bash
$cd ${WUKONG_ROOT}/datagen
$g++ -std=c++11 add_timestamp.cpp -o add_timestamp
$./add_timestamp /path/to/dataset
```
`add_timestamp`会将ID三元组数据集替换为时序ID三元组（五元组）数据集：
```
205039（主语ID）  23（谓词ID）     204607（宾语ID）  1337335004（有效时间区间开始时间的毫秒时间戳）      443247361（有效时间区间截止时间的毫秒时间戳）
205039  23      204699  1544924311      1107554302
205041  1       21      87840508        1023763187
205041  5       131895  119772761       1484157313
205041  22      204527  1365067893      740859041
205041  14      205042  1039202204      1085642062
205041  15      131086  912734282       632043196
205041  23      204538  550812267       367607343
...
```

### 编译及运行
```bash
$cd ${WUKONG_ROOT}/scripts
$./build.sh -DTRDF_MODE=ON
$./run.sh 3
```

### 时序RDF查询语言SPARQL-T

```
SELECT ?X ?Y ?s ?e FROM SNAPSHOT <2007-08-12T22:22:22> WHERE {
    [?s, ?e] ?X  ub:memberOf  ?Y  .
}
```

```
SELECT ?Y ?te WHERE {
    [?ts, ?te) ?X memberOf X-Lab .
    ?Y rdf:type Course .
    ?X takesCourse ?Y .
    FILTER(?ts=1)
}
```

- `FROM SNAPSHOT`关键字（可选）可用来对数据集在某时间点上的快照进行查询
- 花括号内的匹配模式是形如`[start, end) subject predicate object`的五元模式，`start`和`end`可以是常量或变量，用来获取/匹配时序三元组的有效时间数据。为了兼容标准的SPARQL语法，我们规定`[start, end)`部分是可选的。

- 花括号内的过滤器可以对`start`和`end`的取值（如果是变量）按照一定条件进行过滤，例如与变量或常量的大小关系比较等。

### 运行SPARQL-T查询语句

```bash
wukong> sparql -f sparql_query/lubm/time/time1 -v 5
INFO:     Parsing a SPARQL query is done.
INFO:     Parsing time: 94 usec
INFO:     Optimization time: 22 usec
INFO:     The query starts from an index vertex, you could use option -m to accelerate it.
INFO:     (last) result row num: 66565 , col num:2
INFO:     The first 5 rows of results:
1: <http://www.Department10.University1.edu/UndergraduateStudent2>      <http://www.Department10.University1.edu>      1990-01-11T15:57:19                                                  2009-09-18T14:00:31
2: <http://www.Department10.University1.edu/UndergraduateStudent5>      <http://www.Department10.University1.edu>      1974-05-26T23:20:56                                                  2011-03-15T22:37:17
3: <http://www.Department10.University1.edu/UndergraduateStudent9>      <http://www.Department10.University1.edu>      1971-09-16T18:01:56                                                  2010-01-14T21:34:56
4: <http://www.Department10.University1.edu/UndergraduateStudent10>     <http://www.Department10.University1.edu>      1983-01-23T06:48:03                                                  2014-04-03T11:43:57
5: <http://www.Department10.University1.edu/UndergraduateStudent13>     <http://www.Department10.University1.edu>      2006-04-16T14:41:31                                                  2007-08-21T01:14:41
INFO:     (average) latency: 16735 usec
```