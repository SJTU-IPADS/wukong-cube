import random
from random import gauss

# global objects
min_htid = 2
min_vid = 65536
next_htid = 17
next_vid = 425177
next_hid = 0    # not the real hid, just a counter
vid_append_num = 500
vid_target_num = 10000000
hid_target_num = 100000000
he_per_file = 50000
next_file_index = 10

# src/dst dir
src_path = 'hyper_data/statistic.dat'
dst_dir = 'id_graphcube_100m/'

# for normal random
normal_htype = (4093.0, 3793.3)
normal_hsize = (1.5, 107.6)
normal_vcnt = (8.6, 10003)
normal_genV_sigma = 5

# recording each vid occurrence
vid_cnt = {}

# buffer for single data file
buffer = []
buffer_cnt = 0


# --------------util functions--------------
def rand(min, max):
    res = min + int(random.random() * (max - min))
    return res

def rand_htype_cnt():
    global normal_htype
    res = gauss(normal_htype[0], normal_htype[0])
    if res <= 0:
        res = normal_htype[0] * 2 - res
    return int(res)

def rand_hsize_cnt():
    global normal_hsize
    res = gauss(normal_hsize[0], normal_hsize[0])
    if res < 0.95:
        res = normal_hsize[0] * 2 - res
    return int(res)

def rand_voccur_cnt():
    global normal_vcnt
    res = gauss(normal_vcnt[0], normal_vcnt[0])
    if res < 1:
        res = normal_vcnt[0] * 2 - res
    return int(res)

def update_vid_cnt(vid):
    global vid_cnt
    old_cnt = vid_cnt[vid]
    if old_cnt == 1: del vid_cnt[vid]
    else: vid_cnt[vid] = old_cnt - 1

def rand_vids(cnt):
    global vid_cnt, dst_dir, vid_append_num

    vids = list(vid_cnt.keys())
    vid_sz = len(vids)
    # if no more vid to use, generate more
    if vid_sz < cnt + 3:
        generate_vids(vid_append_num, dst_dir)
        vids = list(vid_cnt.keys())
        vid_sz = len(vids)

    gen_vids = []
    last_index = -1
    for i in range(cnt):
        # pick vid
        if last_index == -1:
            # if no last_vid, just generate a random vid
            last_index = rand(0, vid_sz)
            new_vid = vids[last_index]
        else:
            # if has last_vid, generate new vid using normal random(last_vid as mean)
            new_index = int(gauss(last_index, normal_genV_sigma))
            if new_index >= 0 and new_index < vid_sz:
                last_index = new_index
                new_vid = vids[last_index]
            else:
                last_index = rand(0, vid_sz)
                # if last_index < 0 or last_index >= len(vids):
                #     print(last_index)
                #     print(vid_sz)
                #     print(len(vids))
                new_vid = vids[last_index]
        # update meta
        # new_vid = vids[last_index]
        update_vid_cnt(new_vid)
        gen_vids.append(new_vid)
        del vids[last_index]
        vid_sz -= 1
    
    return gen_vids


# --------------main functions--------------
def load_parameters(file_path):
    global normal_htype, normal_hsize, normal_vcnt, normal_hsize_delta, normal_vcnt_delta
    src_file = open(file_path)

    # read src lines
    src_lines = src_file.readlines()

    # iterate each line
    cnt = 0
    for line in src_lines:
        if not line.startswith('mu, sigma ='): continue
        elements = line.replace("=", ",").replace("\n", "").replace(" ", "").split(",")
        if cnt == 0:
            normal_htype = (float(elements[2]), float(elements[3]))
        if cnt == 1:
            normal_hsize = (float(elements[2]) + normal_hsize[0], float(elements[3]) + normal_hsize[1])
        if cnt == 2:
            normal_vcnt = (float(elements[2]) + normal_vcnt[0], float(elements[3]) + normal_vcnt[1])
        cnt += 1
    
    print(normal_htype)
    print(normal_hsize)
    print(normal_vcnt)

def flush_buffer(output_dir):
    global next_file_index, buffer, buffer_cnt
    # generate file name
    file_name = 'hyper_id_uni' + str(next_file_index) + '.nt'
    next_file_index += 1
    print("generating file " + file_name + '...')
    # open file
    file = open(output_dir + file_name, 'w')
    # write file
    for hyperedge in buffer:
        file.write(hyperedge)
    # close file
    file.close()
    # clear buffer
    buffer.clear()
    buffer_cnt = 0

def generate_vids(cnt, output_dir):
    global next_vid, min_vid, vid_cnt

    # open dst file
    output_file = output_dir + 'str_normal'
    mode = 'a'
    if next_vid == min_vid: mode = 'w'
    dst_file = open(output_file, mode)

    # generate new vid
    for i in range(cnt):
        # generate vid
        vid = next_vid
        next_vid += 1
        # initiate vid_cnt
        vid_cnt[vid] = rand_voccur_cnt()
        # print vid
        dst_file.write('Entity' + str(vid) + '\t\t' + str(vid) + '\n')
    
    # reset some old vid
    for i in range(int(cnt/50)):
        old_vid = rand(min_vid, next_vid - cnt)
        append_cnt = int(rand_voccur_cnt() / 5) + 1
        vid_cnt[old_vid] = append_cnt

    # close dst file
    dst_file.close()

def generate_hyperedge(htid):
    global next_hid

    # generate hid
    hid = next_hid
    next_hid += 1
    res = 'HyperEdge' + str(hid) + '\t' + str(htid) + '\t|\t'

    # generate vids
    he_sz = rand_hsize_cnt()
    vids = rand_vids(he_sz)
    for vid in vids:
        res += str(vid) + '\t'

    # generate timestamp
    res += '|\t2018-10-10\t2022-3-10\n'

    # return the hyperedge line(string)
    return res

def generate_hypertype(output_dir):
    global next_htid, min_htid, he_per_file, buffer_cnt, buffer

    # open dst file
    output_file = output_dir + 'hyper_str_index'
    mode = 'a'
    if next_htid == min_htid: mode = 'w'
    dst_file = open(output_file, mode)
    # generate htid
    htid = next_htid
    next_htid += 1
    dst_file.write('HyperType' + str(htid) + '\t\t' + str(htid) + '\n')
    # close dst file
    dst_file.close()

    # generate hyperedges
    he_cnt = rand_htype_cnt()
    for i in range(he_cnt):
        new_hyperedge = generate_hyperedge(htid)
        # if buffer is filled, flush and clear it
        if buffer_cnt == he_per_file:
            flush_buffer(output_dir)
        # append new hpyeredge to buffer
        buffer.append(new_hyperedge)
        buffer_cnt += 1

def gen_data():
    global dst_dir, next_hid, next_vid, hid_target_num, vid_target_num, he_per_file
    # load meta
    # load_parameters(src_path)
    # generate vids
    generate_vids(he_per_file, dst_dir)
    # generate data
    while next_hid < hid_target_num or (next_vid - min_vid) < vid_target_num:
        generate_hypertype(dst_dir)
    if len(buffer) > 0:
        flush_buffer(dst_dir)
    # print result
    print(f'{next_vid - min_vid} Vertex in total.')
    print(f'{next_htid - min_htid} HyperTypes in total.')
    print(f'{next_hid} HyperEdges in total.')
    
gen_data()
# print(vid_cnt)
# print("generate_hyperedges:")
# for i in range(20):
#     print(generate_hyperedge(2))
# print(vid_cnt)

# a = {'a':1, 'b':2, 'c':3, 'd':4}
# b = list(a.keys())
# print(a)
# print(b)

# del b[1]
# print(a)
# print(b)

# del a['c']
# print(a)
# print(b)