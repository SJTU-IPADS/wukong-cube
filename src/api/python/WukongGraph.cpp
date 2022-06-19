#include <algorithm>

#include "client/rpc_client.hpp"
#include "utils/assertion.hpp"

#include "WukongGraph.h"

namespace py = pybind11;

WukongGraph::WukongGraph(std::string host, int port) {
    client.connect_to_server(host, port);
}

WukongGraph::~WukongGraph(){}

py::tuple WukongGraph::RetrieveClusterInfo(int timeout) {
    client.retrieve_cluster_info(timeout);

	return py::make_tuple();
}

py::tuple WukongGraph::ExecuteSPARQLQuery(std::string query_text, int timeout) {
	std::string result_data;

    // Execute the sampling query
    client.execute_sparql_query(query_text, result_data, timeout);

	return py::make_tuple(result_data);
}

void init_wukong_graph(py::module &m) {
  py::class_<WukongGraph>(m, "WukongGraph")
    .def(py::init<std::string, int>())
    .def("retrieve_cluster_info", &WukongGraph::RetrieveClusterInfo, py::arg("timeout") = ConnectTimeoutMs)
    .def("execute_sparql_query", &WukongGraph::ExecuteSPARQLQuery, py::arg("query_text"), py::arg("timeout") = ConnectTimeoutMs);
}
