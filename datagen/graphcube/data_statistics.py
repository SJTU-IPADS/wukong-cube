import os
from random import gauss
from unittest.util import sorted_list_difference
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import math

# src/dst dir
src_dir = 'graphcube_v5/'
dst_file = 'statistic.dat'

htypeCnt = {}   # hypertype -> hyperedge cnt of this hypertype
heSzCnt = {}     # hyperedge sz -> the cnt of hyperedge in this size
vidOccurCnt = {}     # vid -> the occurrences cnt of this vid
vCnt = {}     # vid occurrence sz -> the cnt of vid in this size

# --------------util functions--------------
def record_he_size(size):
    global heSzCnt
    tmp = heSzCnt.get(size)
    if tmp == None: heSzCnt[size] = 1
    else: heSzCnt[size] = tmp + 1

def record_htype_occur(htype):
    global htypeCnt
    tmp = htypeCnt.get(htype)
    if tmp == None: htypeCnt[htype] = 1
    else: htypeCnt[htype] = tmp + 1

def record_vid_ocurr(vid):
    global vCnt
    tmp = vCnt.get(vid)
    if tmp == None: vCnt[vid] = 1
    else: vCnt[vid] = tmp + 1

def transfer_vid_occur():
    global vCnt, vidOccurCnt
    for value in vCnt.values():
        tmp = vidOccurCnt.get(value)
        if tmp == None: vidOccurCnt[value] = 1
        else: vidOccurCnt[value] = tmp + 1

# --------------math functions--------------
def get_normal_distribution_from_list(list):
    # calculate midian as mean
    sorted_list = sorted(list.items(), key=lambda x: x[1])
    length = len(sorted_list)
    if length % 2 == 0: 
        middle = sorted_list[int(length/2-1)][1] + sorted_list[int(length/2)][1]
    else:
        middle = sorted_list[int(length/2)][1]
    
    # calculate standard deviation, remove the first and the last element in the list
    deviation = 0
    # for element in sorted_list[1:-1]:
    for element in sorted_list:
        deviation += math.pow((element[1] - middle), 2)
    
    # if length > 3:
    #     deviation = math.sqrt(deviation / (length - 3))
    # else:
    deviation = math.sqrt(deviation / (length - 1))

    return middle, deviation

def get_normal_distribution_from_cnt(cntList):
    # calculate mean
    total_value = 0
    total_cnt = 0
    for (key, value) in cntList.items():
        total_cnt += value
        total_value += key * value
    mean = total_value / total_cnt
    
    # calculate standard deviation
    deviation = 0
    for (key, value) in cntList.items():
        deviation += math.pow((key - mean), 2) * value
    deviation = math.sqrt(deviation / (total_cnt - 1))

    return mean, deviation

# --------------main functions--------------
def process_file(src_path):
    print("processsing ", src_path, "...")
    src_file = open(src_path)

    # read src lines
    src_lines = src_file.readlines()

    # iterate each line
    for i, line in enumerate(src_lines):
        # if i > 1: break;
        # split line into elements
        elements = line.replace("|\t", "").replace("\n", "").split("\t")
        # get hypertype
        htid = int(elements[1])
        record_htype_occur(htid)
        # get he size
        he_sz = len(elements) - 4
        record_he_size(he_sz)
        # iterate through each vid
        for vid in elements[2:-2]:
            record_vid_ocurr(int(vid))

    # close src file
    src_file.close()

def output_result(dst_path):
    global htypeCnt, heSzCnt, vidOccurCnt

    # print result
    # print("htypeCnt: ", htypeCnt)
    # print("heSzCnt: ", heSzCnt)
    # # print("vCnt: ", vCnt)
    # print("vidOccurCnt: ", vidOccurCnt)

    dst_file = open(dst_path, 'w')

    # print hypertype count
    dst_file.write('htid\tcnt\n')
    for (key,value) in sorted(htypeCnt.items(), key=lambda x: x[0]):
        dst_file.write(str(key) + '\t' + str(value) + '\n')
    mu, sigma = get_normal_distribution_from_list(htypeCnt)
    dst_file.write('mu, sigma = %.1f, %.1f\n' %(mu, sigma))
    dst_file.write('\n')

    # print hyperedge node count
    dst_file.write('he_size\tcnt\n')
    for (key,value) in sorted(heSzCnt.items(), key=lambda x: x[0]):
        dst_file.write(str(key) + '\t' + str(value) + '\n')
    mu, sigma = get_normal_distribution_from_cnt(heSzCnt)
    dst_file.write('mu, sigma = %.1f, %.1f\n' %(mu, sigma))
    dst_file.write('\n')

    # print node occur count
    dst_file.write('vid_ocurr_time\tcnt\n')
    for (key,value) in sorted(vidOccurCnt.items(), key=lambda x: x[0]):
        dst_file.write(str(key) + '\t' + str(value) + '\n')
    mu, sigma = get_normal_distribution_from_cnt(vidOccurCnt)
    dst_file.write('mu, sigma = %.1f, %.1f\n' %(mu, sigma))
    dst_file.write('\n')

    # close dst file
    dst_file.close()

def process_dir(src):
    dir_list = os.listdir(src)
    print(len(dir_list), " src Files in '", src, "' :")

    # tranfer each data file
    for src_file in dir_list:
        if not src_file.endswith(".nt"): continue
        process_file(src + src_file)
    
    # transfer vid count
    transfer_vid_occur()

    # output result
    output_result(src_dir + dst_file)
    



process_dir(src_dir)

# print(gauss(mu1, sigma1))
# x = np.arange(0, 100, 1)
# y1 = stats.norm.pdf(x, mu1, sigma1)#正态分布密度函数在x处的取值
# plt.plot(x,y1,'r',label='pdf')
# y2 = stats.norm.cdf(x, mu1, sigma1)#正态分布的函数值
# plt.plot(x,y2,'g',label='cdf')
# plt.legend(loc='best')
# plt.title('Normal:$\mu$ = %.1f, $\sigma^2$ = %.1f' %(mu1, sigma1))
# plt.xlabel('x')
# plt.show()
# plt.savefig('result.jpg')
