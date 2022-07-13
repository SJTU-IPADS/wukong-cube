import os

# src/dst dir
src_dir = 'hyper_data_origin/'
dst_dir = 'hyper_data_rdf2/'

# __PREDICATE__   0
# <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>       1
# <http://www.w3.org/1999/02/22-rdf-syntax-ns#hyperedge>       2
# <http://www.w3.org/1999/02/22-rdf-syntax-ns#hypertype1>       3
# ...

# global objects
str2id_type = {}
str2id_vertex = {}
str2id_edge = {}
next_type_id = 4
next_vertex_id = (1 << 17)
next_edge_id = (1 << 20)

def get_edge_id(str):
    global str2id_edge, next_edge_id
    id = 0
    if str in str2id_edge:
        # print(str)
        id = str2id_edge[str]
    else:
        id = next_edge_id
        str2id_edge[str] = id
        next_edge_id += 1
    assert(id >= (1 << 19))
    return id

def get_vertex_id(str):
    global str2id_vertex, next_vertex_id
    id = 0
    if str in str2id_vertex:
        id = str2id_vertex[str]
    else:
        id = next_vertex_id
        str2id_vertex[str] = id
        next_vertex_id += 1
    assert(id >= (1 << 16))
    return id

def get_type_id(str):
    global str2id_type, next_type_id
    id = 0
    if str in str2id_type:
        id = str2id_type[str]
    else:
        id = next_type_id
        str2id_type[str] = id
        next_type_id += 1
    assert(id < (1 << 16))
    return id

def gen_new_line(elements):
    assert(len(elements) >= 4)
    # newline = he_name he_type_id | vid0 vid1 ... | time1 time2
    he_name = (elements[1] + "的" + elements[0]).replace(' ', '_')
    he_id = get_edge_id(he_name)
    he_type_id = get_type_id(elements[0])
    newline = str(he_id) + '\t1\t' + str(he_type_id) + '\n'
    for vertex in elements[4:]:
        newline += str(he_id) + '\t2\t' + str(get_vertex_id(vertex)) + '\n'
    return newline

def output_str_index(dst_path):
    global str2id_type
    # open dst files
    dst_file = open(dst_path, 'w')
    dst_file_ch = open(dst_path + '_ch', 'w')
    # output
    dst_file.write('__PREDICATE__\t\t0\n')
    dst_file.write('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t\t1\n')
    dst_file.write('<http://www.w3.org/1999/02/22-rdf-syntax-ns#hyperedge>\t\t2\n')     # predicate type
    dst_file.write('<http://www.w3.org/1999/02/22-rdf-syntax-ns#entity>\t\t3\n')     # vertex type
    for type, id in str2id_type.items():
        type = type.replace(' ', '_')
        iri = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#hypertype" + str(id) + ">"
        dst_file.write(iri + '\t\t' + str(id) + '\n')
        dst_file_ch.write(type + '\t\t' + str(id) + '\n')
    # close dst file
    dst_file.close()
    dst_file_ch.close()

def output_normal_index(dst_path):
    global str2id_vertex, str2id_edge
    # open dst files
    dst_file = open(dst_path, 'w')
    dst_file_ch = open(dst_path + '_ch', 'w')
    # output vertex
    for name, id in str2id_vertex.items():
        name = name.replace(' ', '_')
        iri = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#entity" + str(id) + ">"
        dst_file.write(iri + '\t\t' + str(id) + '\n')
        dst_file_ch.write(name + '\t\t' + str(id) + '\n')
    # output edge
    for name, id in str2id_edge.items():
        name = name.replace(' ', '_')
        iri = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#edge" + str(id) + ">"
        dst_file.write(iri + '\t\t' + str(id) + '\n')
        dst_file_ch.write(name + '\t\t' + str(id) + '\n')
    # close dst file
    dst_file.close()
    dst_file_ch.close()

def output_vertex_type(dst_path):
    global str2id_vertex
    # open dst files
    dst_file = open(dst_path, 'w')
    # output vertex
    for _, id in str2id_vertex.items():
        dst_file.write(str(id) + '\t1\t3\n')
    # close dst file
    dst_file.close()

def transfer(src_path, dst_path):
    # open src and dst files
    src_file = open(src_path)
    dst_file = open(dst_path, 'w')

    # read src lines
    src_lines = src_file.readlines()
    src_count = len(src_lines)

    # record last line information
    last_elements = []

    dst_count = 0
    for line in src_lines:
        # if dst_count > 10: continue;
        # split line into elements for reordering
        elements = line.replace("{", "").replace("}", "").replace(".", "").replace("\n", "").split(",")
        # check if last line is the same type+company with current line
        if len(last_elements) > 0:
            if not (elements[0] == last_elements[0] and elements[1] == last_elements[1]):
                # print output
                dst_file.write(gen_new_line(last_elements))
                # increate dst_counter
                dst_count += 1
        # update last info
        last_elements = elements
    
    # always transfer the last line
    dst_file.write(gen_new_line(elements))
    dst_count += 1

    print(f'{src_path}: {src_count} to {dst_count}.')

    # close src and dst file
    src_file.close()
    dst_file.close()

    return dst_count

def transfer_dir(src, dst):
    # Get the list of all files and directories
    dir_list = os.listdir(src)
    print("src Files in '", src, "' :")

    # tranfer each data file
    file_count = 0
    edge_count = 0
    for src_file in dir_list:
        print(src_file)
        # if not src_file == "title.txt": continue
        src_path = src + src_file
        dst_path = dst + 'id_uni' + str(file_count) + '.nt'
        edge_count += transfer(src_path, dst_path)
        file_count += 1

    # output str_index file
    output_str_index(dst + 'str_index')
    # output str_normal file
    output_normal_index(dst + 'str_normal')
    # output normal vertex type file
    output_vertex_type(dst + 'id_uni' + str(file_count) + '.nt')

    # print counts
    global next_type_id, next_vertex_id, next_edge_id
    vertex_count = next_vertex_id - (1 << 17)
    edge_count = next_edge_id - (1 << 20)
    type_count = next_type_id - 4
    print(f'{vertex_count} Vertex in total.')
    print(f'{type_count} HyperTypes in total.')
    print(f'{edge_count} HyperEdges in total.')

transfer_dir(src_dir, dst_dir)
