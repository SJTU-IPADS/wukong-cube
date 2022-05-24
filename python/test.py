import WukongGraph
import datetime

graph = WukongGraph.WukongGraph("0.0.0.0", 6577)
graph.retrieve_cluster_info(100)

def execute_query(qno):
    fname = "../scripts/sparql_query/lubm/basic/lubm_q" + str(qno) 
    print(fname)
    f = open(fname)
    qcontent = f.read()

    begin_time = datetime.datetime.now()
    _ = graph.execute_sparql_query(qcontent, 10000)
    end_time = datetime.datetime.now()
    d_time = end_time - begin_time
    # print("\tquery result: \t", result, "\n")
    print("\tlatency: ", d_time.microseconds, " usec.")

for i in range(6):
    execute_query(i+1)
