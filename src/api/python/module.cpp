#include <pybind11/pybind11.h>
#include "WukongGraph.h"

namespace py = pybind11;

void init_wukong_graph(py::module &);

PYBIND11_MODULE(WukongGraph, m) {
  init_wukong_graph(m);
}
