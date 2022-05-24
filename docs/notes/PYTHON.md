# Wukong Python Library

## Table of Contents
### Setup 
- [Code structure](#code)
- [Prepare for the env](#env)
- [Build and install](#build)

### Python Lib API
- [Initialize and build connection](#init)
- [Check and retrieve cluster info](#check)
- [Run queries](#query)

---

## Setup

<a name="code"></a>

### Code structure
All files related to **Wukong Python Library** are in `$WUKONG_ROOT/python` directory.
- `$WUKONG_ROOT/python/cpp` is reserved for further development, currently is unused.
- `$WUKONG_ROOT/python/pybind11` is the implementation of the python lib. Our python lib is written in cpp code, using RPC to communicate with the RPCClient in Wukong. The API declearation is in `WukongGraph.h`, and the implementation in `WukongGraph.cpp`.
- `$WUKONG_ROOT/python/build.sh` is a build script for compile and install python lib.
- `$WUKONG_ROOT/python/setup.py` is a helper script provided by [pybind11](https://github.com/pybind/pybind11), try not modify it.
- `$WUKONG_ROOT/python/test.py` is a basic test file, and a sample of using **Wukong Python Library**.

<a name="env"></a>

### Prepare for the env

- **Python**: minimum required is "3.6", [conda](https://docs.conda.io/en/latest/miniconda.html) environment is recommanded for convenience.
- **Pybind**: this requirement has been added into `.gitmodules`, just remember to run `git submodule update` before you try to build the python lib.
- **nlohmannJson**: this requirement has been added into `.gitmodules`, just remember to run `git submodule update` before you try to build wukong.


<a name="build"></a>

### Build and install

Once the environment is ready, run the commands below and you get **Wukong Python Library** installed on your machine.
```
cd $WUKONG_ROOT/python
./build.sh
```


## Python Lib API

<a name="init"></a>

### Initialize and build connection

1. Configuring and running Wukong, if you have any question, refer to [TUTORIAL](TUTORIALS.md).
2. Import **WukongGraph** Lib in your python environment.
3. Use `WukongGraph.WukongGraph(ADDR, PORT)` to initialize your WukongGraph instance, where the `ADDR` is the ip of your Wukong(initiated in the first step), and the `PORT` is the listening port (Wukong::server_port_base + 1)of Wukong for RPC requests.

<a name="check"></a>

### Check and retrieve cluster info

Use `retrieve_cluster_info()` to check your connection with Wukong program. The API takes no parameters and prints Wukong cluster information.
```
>>> import WukongGraph
>>> graph = WukongGraph.WukongGraph("0.0.0.0", 6577)
>>> graph.retrieve_cluster_info()
Reply:cluster info...
```

<a name="query"></a>

### Run queries

Use `execute_sparql_query(req)` to execute a SPARQL query on Wukong. The API takes a string parameter(a SPARQL query), and returns a result string(in JSON format).
- **SELECT** query
    
    If you run a SELECT query like `sparql_query/lubm/basic/lubm_q1`, just pass the query text into `execute_sparql_query`:
    ```
    # the content of lubm_q1:
    # SELECT ?X ?Y ?Z WHERE {
    #	?Y  rdf:type  ub:University  .
    #	?X  ub:undergraduateDegreeFrom  ?Y  .
    #	?X  rdf:type  ub:GraduateStudent  .
    #	?X  ub:memberOf  ?Z  .
    #	?Z  ub:subOrganizationOf  ?Y  .
    #	?Z  rdf:type  ub:Department  .
    # }

    f = open("sparql_query/lubm/basic/lubm_q1")
    query_text = f.read()
    result = graph.execute_sparql_query(query_text);
    ```
    And the result will be like:
    ```
    // the return value of a SELECT query
    {
        "StatusMsg":0,		// 0 means success
        "Result":{
            "Type":"SELECT",
            "Size":{
                "Col":3,
                "Row":106
            },
            "Data":[
                {
                    "X":{
                    "type":"STRING_t",
                    "value":"<http://www.Department8.University2.edu>"
                    },
                    "Y":{
                    "type":"STRING_t",
                    "value":"<http://www.Department8.University2.edu/GraduateStudent61>"
                    },
                    "Z":{
                    "type":"STRING_t",
                    "value":"<http://www.University2.edu>"
                    }
                },
                {
                    "X":{
                    "type":"STRING_t",
                    "value":"<http://www.Department14.University2.edu>"
                    },
                    "Y":{
                    "type":"STRING_t",
                    "value":"<http://www.Department14.University2.edu/GraduateStudent101>"
                    },
                    "Z":{
                    "type":"STRING_t",
                    "value":"<http://www.University2.edu>"
                    }
                },
                ...
            ]
        }
    }
    ```
- **ASK** query
    If you run a ASK query like `sparql_query/lubm/ask/lubm_q1`,  the result will be like:
    ```
    // the return value of a ASK query
    {
        "StatusMsg": 0,
        "Result":{
            "Type":"ASK",
            "Value": true
        }
    }
    ```