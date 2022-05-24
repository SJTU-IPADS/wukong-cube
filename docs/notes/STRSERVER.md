# Wukong Standalone String Server

## Table of Contents
- [Why Standalone String Server?](#why)
- [Code structure](#code)
- [Config and Run](#env)


<a name="why"></a>
## Why Standalone String Server?

In distributed scenarios, each wukong instance(on different nodes) keeps a full copy of the Str-ID mapping. But when the dataset is too large, a single machine may not be able to handle graph data plus a full copy of the Str-ID mapping. Thus we present standalone string server, which means to dedicate one machine in the cluster as global String Server, and other machines make Str-ID conversions by sending RPC requests to it.

<a name="code"></a>
## Code structure

All files related to **Wukong Standalone String Server** are in `$WUKONG_ROOT/src/stringserver` directory.

- `$WUKONG_ROOT/src/stringserver/sting_mapping.hpp`: the base class for StringServer and StringCache, provide `str2id` and `id2str` API for upper modules.
- `$WUKONG_ROOT/src/stringserver/string_server.hpp`: used when each wukong instance keeps a full copy of Str-ID mapping, stores mapping on local machine.
- `$WUKONG_ROOT/src/stringserver/string_cache.hpp`: used when Standalone String Server is on, use RPC to query `str2id` and `id2str` queries.
- `$WUKONG_ROOT/src/stringserver/string_proxy.hpp`: the RPC server which provides a Str-ID conversion service.
- `$WUKONG_ROOT/src/stringserver/sscache_request.hpp`: the RPC request data structure between StringCache and StringProxy.
- `$WUKONG_ROOT/src/stringserver/run_string_server.cpp`: the main routine to start a string server.
- `$WUKONG_ROOT/src/stringserver/test_string_client.cpp`: a test file for Str-ID service, should be tested on dataset LUBM40.



<a name="env"></a>
## Config and Run

To use Standalone String Server, you need to add the following two  parameters into `$WUKONG_ROOT/scripts/config`.

```
global_enable_standalone_str_server     1
global_standalone_str_server_addr       [ip]:[port]
```

Before launching distributed wukong, don't forget to start string server first.

```
cd $WUKONG_ROOT/scripts
../build/string_server [port] /path/to/your/dataset
```

