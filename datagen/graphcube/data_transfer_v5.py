import os

# src/dst dir
src_dir = 'graphcube_raw/'
dst_dir = 'graphcube_30_v5_complete/'

# global objects
str2id_type = {}
str2id_vertex = {}
next_type_id = 2
next_vertex_id = (1 << 16)

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

def gen_new_line(info, nodes):
    # print(info)
    # print(nodes)
    assert(len(info) == 2)
    assert(len(nodes) >= 0)
    # newline = he_name he_type_id | vid0 vid1 ... | time1 time2
    he_name = info[1] + "的" + info[0]
    he_type_id = get_type_id(info[0])
    newline = he_name.replace(' ', '_') + '\t' + str(he_type_id) + "\t|\t"
    for vertex in nodes:
        newline += str(get_vertex_id(vertex)) + '\t'
    newline += "|\t2022-06-30\t2022-07-01\n"
    return newline

def output_str_index(dst_path):
    # open dst files
    dst_file = open(dst_path, 'w')
    # output
    dst_file.write('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t\t0\n')
    dst_file.write('<http://www.w3.org/1999/02/22-rdf-syntax-ns#hypertype>\t\t1\n')
    # close dst file
    dst_file.close()

def output_hyper_index(dst_path):
    # open dst files
    dst_file = open(dst_path, 'w')
    # output
    for type, id in str2id_type.items():
        type = type.replace(' ', '_')
        dst_file.write(type + '\t\t' + str(id) + '\n')
    # close dst file
    dst_file.close()

def output_normal_index(dst_path):
    # open dst files
    dst_file = open(dst_path, 'w')
    # output
    for name, id in str2id_vertex.items():
        name = name.replace(' ', '_')
        dst_file.write(name + '\t\t' + str(id) + '\n')
    # close dst file
    dst_file.close()

def transfer(src_path, dst_path):
    # open src and dst files
    src_file = open(src_path)
    dst_file = open(dst_path, 'w')

    # read src lines
    src_lines = src_file.readlines()
    src_count = len(src_lines)

    # record current hyper edge information
    current_elements = list()
    current_nodes = set()

    dst_count = 0
    for line in src_lines:
        # if dst_count > 2: continue
        # split line into elements for reordering
        elements = line.replace("{", "").replace("}", "").replace(".", "").replace("\n", "").split(",")
        # skip unqualified lines
        if len(elements) < 4: continue
        # check if this line is the same type+company with current hyper edge
        #   if so, add new nodes into current_nodes
        #   if not, generate new line and update current hyperedge info
        if len(current_elements) > 0 and (elements[0] != current_elements[0] or elements[1] != current_elements[1]):
            # print(f'\n\t{dst_count}:')
            # print output
            dst_file.write(gen_new_line(current_elements, current_nodes))
            # increate dst_counter
            dst_count += 1
            # clear current hyperedge nodes
            current_nodes.clear()
        # update current hyperedge info
        current_elements = elements[0:2]
        for n in elements[4:]:
            if n != "none": current_nodes.add(n)
    
    # always transfer the last line
    dst_file.write(gen_new_line(current_elements, current_nodes))
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
        # if not src_file == "上市公司供应商.txt": continue
        src_path = src + src_file
        dst_path = dst + 'hyper_id_uni' + str(file_count) + '.nt'
        edge_count += transfer(src_path, dst_path)
        file_count += 1

    # output str_index file
    output_str_index(dst + 'str_index')
    # output hyper_str_index file
    output_hyper_index(dst + 'hyper_str_index')
    # output str_normal file
    output_normal_index(dst + 'str_normal')

    # print counts
    global next_type_id, next_vertex_id
    vertex_count = next_vertex_id - (1 << 16)
    type_count = next_type_id - 2
    print(f'{vertex_count} Vertex in total.')
    print(f'{type_count} HyperTypes in total.')
    print(f'{edge_count} HyperEdges in total.')

transfer_dir(src_dir, dst_dir)
