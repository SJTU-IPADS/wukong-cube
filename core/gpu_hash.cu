/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

#include "gpu_hash.hpp"
#include <thrust/execution_policy.h>

#define ASSOCIATIVITY 8

/*********************************************
 *                                           *
 *                Utilities                  *
 *                                           *
 *********************************************/
__device__
static uint64_t myhash(ikey_t lkey)
{
    uint64_t r = 0;
    r += lkey.vid;
    r <<= NBITS_IDX;
    r += lkey.pid;
    r <<= NBITS_DIR;
    r += lkey.dir;

    uint64_t key = r;
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

__device__
uint64_t map_location_on_shards(uint64_t offset, uint64_t *head_list, uint64_t shard_sz)
{
    return head_list[offset / shard_sz] + offset % shard_sz;
}

/*********************************************
 *                                           *
 *                Query functions            *
 *                                           *
 *********************************************/

__global__
void d_generate_key_list_i2u(sid_t *result_table,
                                int index_vertex,
                                int direction,
                                ikey_t *key_list,
                                int size)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if(index<size) {
        ikey_t r = ikey_t(0,index_vertex,direction);
        key_list[index] = r;
    }
}


void generate_key_list_i2u(sid_t *result_table,
                       int index_vertex,
                       int direction,
                       void *key_list,
                       int query_size,
                       cudaStream_t stream_id)
{
    d_generate_key_list_i2u<<<WUKONG_GET_BLOCKS(query_size), WUKONG_CUDA_NUM_THREADS, 0, stream_id >>>(result_table, index_vertex, direction, (ikey_t*) key_list, query_size);
}




__global__
void k_generate_key_list_k2u(sid_t *result_table,
                                ikey_t *key_list,
                                int var2col_start,
                                int direction,
                                int predict,
                                int col_num,
                                int row_num)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < row_num) {
        int prev_id = result_table[index * col_num + var2col_start];
        // if (index < 15)
            // printf("prev_id: %d\n", prev_id);
        ikey_t r = ikey_t(prev_id,predict,direction);
        key_list[index] = r;
    }
}

void gpu_generate_key_list_k2u(GPUEngineParam &param, cudaStream_t stream)
{

    assert(param.query.row_num > 0);
    assert(param.query.var2col_start >= 0);

    k_generate_key_list_k2u<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(
                param.gpu.d_in_rbuf,
                param.gpu.d_key_list,
                param.query.var2col_start, // param.query.start_vid,
                param.query.dir,
                param.query.pid,
                param.query.col_num,
                param.query.row_num
                );
}




__device__
void d_generate_key_list_k2u(int index,
                                sid_t *result_table,
                                ikey_t *key_list,
                                int var2col,
                                int direction,
                                int predict,
                                int col_num,
                                int row_num)
{
    int prev_id = result_table[index * col_num + var2col];
    ikey_t r = ikey_t(prev_id,predict,direction);
    key_list[index] = r;
}


__global__
void k_get_slot_id_list(vertex_t* d_vertex_addr,
                 ikey_t* d_key_list,
                 uint64_t* d_slot_id_list,
                 uint64_t* d_bucket_id_list,
                 vertex_t* d_vertex_list,
                 ikey_t empty_key,
                 rdf_segment_meta_t *seg_meta,
                 uint64_t* vertex_headers,
                 uint64_t pred_vertex_shard_size,
                 int query_size)

{

    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < query_size) {
        ikey_t key =  d_key_list[index];
        uint64_t bucket_id=map_location_on_shards(myhash(key) % seg_meta->num_buckets,
                                                  vertex_headers,
                                                  pred_vertex_shard_size);
        while (true) {
            for (uint64_t i=0;i<ASSOCIATIVITY;i++) {
                uint64_t slot_id = bucket_id * ASSOCIATIVITY + i;
                if (i < ASSOCIATIVITY - 1) {
                    // data part
                    if (d_vertex_addr[slot_id].key == key) {
                        // we found it
                        d_slot_id_list[index] = slot_id;
                        // TODO debug
                        d_vertex_list[index] = d_vertex_addr[slot_id];
                        d_bucket_id_list[index] = bucket_id;
                        return;
                    }
                } else {
                    if (!(d_vertex_addr[slot_id].key == empty_key)) {
                        // next pointer
                        // uint64_t next_bucket_id = d_vertex_addr[slot_id].key.vid-pred_metas[key.pid].indrct_hdr_start+pred_metas[key.pid].partition_sz;
                        uint64_t next_bucket_id = d_vertex_addr[slot_id].key.vid - seg_meta->ext_bucket_list[0].start + seg_meta->num_buckets;
                        bucket_id = map_location_on_shards(next_bucket_id,
                                                         vertex_headers,
                                                         pred_vertex_shard_size);
                        break;
                    } else {
                        d_slot_id_list[index] = (uint64_t)(-1);
                        return;
                    }
                }
            }
        }
    }
}


__device__
void d_get_slot_id_list(int index,
                vertex_t* d_vertex_addr,
                ikey_t* d_key_list,
                uint64_t* d_slot_id_list,
                ikey_t empty_key,
                rdf_segment_meta_t *seg_meta,
                uint64_t* vertex_headers,
                uint64_t pred_vertex_shard_size,
                int query_size)
{
    ikey_t key =  d_key_list[index];
    uint64_t bucket_id=map_location_on_shards(myhash(key) % seg_meta->num_buckets,
                                              vertex_headers,
                                              pred_vertex_shard_size);
    while (true) {
        for (uint64_t i=0;i<ASSOCIATIVITY;i++) {
            uint64_t slot_id = bucket_id * ASSOCIATIVITY + i;
            if (i < ASSOCIATIVITY - 1) {
                // data part
                if (d_vertex_addr[slot_id].key == d_key_list[index]) {
                    // we found it
                    d_slot_id_list[index] = slot_id;
                    return;
                }
            } else {
                if (!(d_vertex_addr[slot_id].key == empty_key)) {
                    // next pointer
                    uint64_t next_bucket_id = d_vertex_addr[slot_id].key.vid - seg_meta->ext_bucket_list[0].start + seg_meta->num_buckets;
                    bucket_id = map_location_on_shards(next_bucket_id,
                                                     vertex_headers,
                                                     pred_vertex_shard_size);
                    break;
                } else {
                    d_slot_id_list[index] = (uint64_t)(-1);
                    return;
                }
            }
        }
    }
}

// done
void gpu_get_slot_id_list(GPUEngineParam &param, cudaStream_t stream)
{
    assert(param.query.row_num > 0);
    assert(param.query.var2col_start >= 0);

    ikey_t empty_key = ikey_t();

    k_get_slot_id_list<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(
            param.gpu.d_vertex_addr,
            param.gpu.d_key_list,   // (ikey_t*)d_key_list,
            param.gpu.d_slot_id_list,  // d_slot_id_list,
            param.gpu.d_bucket_id_list,
            param.gpu.d_vertex_list,    // TODO for debug
            empty_key, // empty_key,
            param.gpu.d_segment_meta,// pred_metas,
            param.gpu.d_vertex_mapping, // vertex_headers,
            param.gpu.vertex_block_sz, // pred_vertex_shard_size,
            param.query.row_num); // query_size);
}

__global__
void k_get_edge_list(uint64_t *slot_id_list,
                    vertex_t *d_vertex_addr,
                    edge_t *d_edge_addr,
                    int *index_list,
                    int *index_list_mirror,
                    uint64_t *off_list,
                    uint64_t pred_orin_edge_start,
                    uint64_t* edge_headers,
                    uint64_t pred_edge_shard_size,
                    int query_size)
{
    // int index = blockIdx.x * blockDim.x * blockDim.y
                // + threadIdx.y * blockDim.x + threadIdx.x;
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if(index<query_size)
    {
        uint64_t id = slot_id_list[index];
        iptr_t r = d_vertex_addr[id].ptr;
        //if (index<10)
        //printf("r.size:%d\n",r.size);
        index_list_mirror[index] = r.size;
        //off_list[index] = map_location_on_shards(r.off-pred_orin_edge_start,
        //                                         edge_headers,
        //                                         pred_edge_shard_size);
        off_list[index] = r.off-pred_orin_edge_start;
   }
}


__device__
void d_get_edge_list(int index,
                    uint64_t *slot_id_list,
                    vertex_t *d_vertex_addr,
                    int *index_list,
                    int *index_list_mirror,
                    uint64_t *off_list,
                    uint64_t pred_orin_edge_start,
                    uint64_t* edge_headers,
                    uint64_t pred_edge_shard_size,
                    int query_size)
{
    uint64_t id = slot_id_list[index];
    iptr_t r = d_vertex_addr[id].ptr;
    //if (index<10)
    //printf("r.size:%d\n",r.size);
    index_list_mirror[index] = r.size;
    //off_list[index] = map_location_on_shards(r.off-pred_orin_edge_start,
    //                                         edge_headers,
    //                                         pred_edge_shard_size);
    off_list[index] = r.off-pred_orin_edge_start;

}

// done
void gpu_get_edge_list(GPUEngineParam &param, cudaStream_t stream)
{

    assert(param.query.row_num > 0);

    k_get_edge_list<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(
                    param.gpu.d_slot_id_list,
                    param.gpu.d_vertex_addr, // (vertex_t*)d_vertex_addr,
                    param.gpu.d_edge_addr, // for debug
                    param.gpu.d_prefix_sum_list, // index_list,
                    param.gpu.d_edge_size_list, // index_list_mirror,
                    param.gpu.d_offset_list, // ptr_list,
                    param.query.segment_edge_start, // pred_orin_edge_start,
                    param.gpu.d_edge_mapping, // edge_headers,
                    param.gpu.edge_block_sz, // pred_edge_shard_size,
                    param.query.row_num); // query_size);

} 




__global__
void k_get_edge_list_k2c(
                    uint64_t *slot_id_list,
                    vertex_t *d_vertex_addr,
                    int *index_list,
                    int *index_list_mirror,
                    uint64_t *offset_list,
                    edge_t *edge_addr,
                    int64_t end,
                    uint64_t pred_orin_edge_start,
                    uint64_t* edge_headers,
                    uint64_t pred_edge_shard_size,
                    int query_size)
{

    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if(index<query_size) {
        uint64_t id = slot_id_list[index];
        iptr_t r = d_vertex_addr[id].ptr;

        index_list_mirror[index] = 0;
        offset_list[index] =r.off-pred_orin_edge_start;
        for (int k=0;k<r.size;k++){
            uint64_t ptr = map_location_on_shards(r.off-pred_orin_edge_start+k,
                                                  edge_headers,
                                                  pred_edge_shard_size);
            if (edge_addr[ptr].val==end) {
                index_list_mirror[index] = 1;
                break;
            }
        }
    }
}

// Siyuan: 2018.9.23 added
void gpu_get_edge_list_k2c(GPUEngineParam &param, cudaStream_t stream)
{

    assert(param.query.row_num > 0);

    k_get_edge_list_k2c<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(
                    param.gpu.d_slot_id_list,
                    param.gpu.d_vertex_addr, // (vertex_t*)d_vertex_addr,
                    param.gpu.d_prefix_sum_list, // index_list,
                    param.gpu.d_edge_size_list, // index_list_mirror,
                    param.gpu.d_offset_list, // ptr_list,
                    param.gpu.d_edge_addr,
                    param.query.end_vid,
                    param.query.segment_edge_start, // pred_orin_edge_start,
                    param.gpu.d_edge_mapping, // edge_headers,
                    param.gpu.edge_block_sz, // pred_edge_shard_size,
                    param.query.row_num); // query_size);
}






__global__
void k_get_edge_list_k2k(uint64_t *slot_id_list,
                    vertex_t *d_vertex_addr,
                    int *index_list,
                    int *index_list_mirror,
                    uint64_t *ptr_list,
                    int query_size,
                    edge_t *edge_addr,
                    sid_t *result_table,
                    int col_num,
                    int var2col_end,
                    uint64_t pred_orin_edge_start,
                    uint64_t* edge_headers,
                    uint64_t pred_edge_shard_size)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if(index<query_size)
    {
        uint64_t id = slot_id_list[index];
        iptr_t r = d_vertex_addr[id].ptr;

        index_list_mirror[index] = 0;

        sid_t end_id = result_table[index * col_num + var2col_end];
        ptr_list[index] = r.off-pred_orin_edge_start;
        for(int k=0;k<r.size;k++){
            uint64_t ptr = map_location_on_shards(r.off-pred_orin_edge_start+k,
                                                  edge_headers,
                                                  pred_edge_shard_size);

            if (edge_addr[ptr].val == end_id) {
                index_list_mirror[index] = 1;
                break;
            }
        }
   }


}

void gpu_get_edge_list_k2k(GPUEngineParam &param, cudaStream_t stream)
{

    assert(param.query.row_num > 0);
    assert(param.query.var2col_end >= 0);

    k_get_edge_list_k2k<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(
                    param.gpu.d_slot_id_list, // slot_id_list,
                    param.gpu.d_vertex_addr, // (vertex_t*)d_vertex_addr,
                    param.gpu.d_prefix_sum_list, // index_list,
                    param.gpu.d_edge_size_list, // index_list_mirror,
                    param.gpu.d_offset_list, // ptr_list,
                    param.query.row_num, // query_size,
                    param.gpu.d_edge_addr, // (edge_t*)edge_addr,
                    param.gpu.d_in_rbuf, // result_table,
                    param.query.col_num, // col_num,
                    param.query.var2col_end, // end,
                    param.query.segment_edge_start, // pred_orin_edge_start,
                    param.gpu.d_edge_mapping, // edge_headers,
                    param.gpu.edge_block_sz); // pred_edge_shard_size);

}


__device__
void d_get_edge_list_k2c(int index,
                    uint64_t *slot_id_list,
                    vertex_t *d_vertex_addr,
                    int *index_list,
                    int *index_list_mirror,
                    uint64_t *ptr_list,
                    edge_t *edge_addr,
                    int64_t end,
                    uint64_t pred_orin_edge_start,
                    uint64_t* edge_headers,
                    uint64_t pred_edge_shard_size,
                    int query_size)
{
    uint64_t id = slot_id_list[index];
    iptr_t r = d_vertex_addr[id].ptr;

    index_list_mirror[index] = 0;
    ptr_list[index] =r.off-pred_orin_edge_start;
    for (int k=0;k<r.size;k++){
        uint64_t ptr = map_location_on_shards(r.off-pred_orin_edge_start+k,
                                              edge_headers,
                                              pred_edge_shard_size);
        if (edge_addr[ptr].val==end) {
            index_list_mirror[index] = 1;
            break;
        }
    }
}

// done
__global__
void k_update_result_buf_i2u(sid_t *result_table,
                                  sid_t *updated_result_table,
                                  int *index_list,
                                  uint64_t *ptr_list,
                                  edge_t *edge_addr,
                                  uint64_t* edge_headers,
                                  uint64_t pred_edge_shard_size)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    int edge_num = 0;
    edge_num = index_list[0];

    if(index<edge_num) {
            uint64_t ptr = map_location_on_shards(ptr_list[0]+index,
                                                  edge_headers,
                                                  pred_edge_shard_size);
            //printf("ptr:%d\n",(&(edge_addr[ptr])+index)->val);
            updated_result_table[index] = edge_addr[ptr].val;
    }

}

// done
int gpu_update_result_buf_i2u(GPUEngineParam& param, cudaStream_t stream)
{
    int table_size = 0;//index_list[query_size-1];
    CUDA_ASSERT(cudaMemcpyAsync(&table_size,
               param.gpu.d_prefix_sum_list,
               sizeof(int),
               cudaMemcpyDeviceToHost, stream));


    k_update_result_buf_i2u<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(param.gpu.d_in_rbuf,
         param.gpu.d_out_rbuf,
         param.gpu.d_prefix_sum_list, //index_list,
         param.gpu.d_offset_list, // ptr_list,
         param.gpu.d_edge_addr,  // (edge_t*)edge_addr,
         param.gpu.d_edge_mapping, // edge_headers,
         param.gpu.edge_block_sz); // pred_edge_shard_size);

    CUDA_ASSERT( cudaStreamSynchronize(stream) );
    return table_size;
}





// done
__global__
void k_update_result_buf_k2k(sid_t *result_table,
                                  sid_t *updated_result_table,
                                  int *index_list,
                                  uint64_t *ptr_list,
                                  int column_num,
                                  edge_t *edge_addr,
                                  int end,
                                  int query_size)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if(index<query_size) {
        int edge_num = 0,start=0;
        if(index==0) {
            edge_num = index_list[index];
            start = 0;
        }
        else {
            edge_num = index_list[index] - index_list[index - 1];
            start = column_num*index_list[index - 1];
        }
        sid_t buff[20];
        for(int c=0;c<column_num;c++){
            buff[c] = result_table[column_num*index+c];
        }
        for(int k=0;k<edge_num;k++){
            for(int c=0;c<column_num;c++){
                updated_result_table[start+c] = buff[c];//result_table[column_num*index+c];
            }
        }
    }
}

// done
int gpu_update_result_buf_k2k(GPUEngineParam& param, cudaStream_t stream)
{
    int table_size = 0;//index_list[query_size-1];
    CUDA_ASSERT( cudaMemcpy(&table_size,
               param.gpu.d_prefix_sum_list + param.query.row_num - 1,
               sizeof(int),
               cudaMemcpyDeviceToHost) );//, stream));

    k_update_result_buf_k2k<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS>>>(
         param.gpu.d_in_rbuf,
         param.gpu.d_out_rbuf,
         param.gpu.d_prefix_sum_list,
         param.gpu.d_offset_list,
         param.query.col_num,
         param.gpu.d_edge_addr,
         param.query.end_vid,
         param.query.row_num);

    // CUDA_ASSERT( cudaStreamSynchronize(stream) );
    return table_size * param.query.col_num;
}

int gpu_update_result_buf_k2c(GPUEngineParam& param, cudaStream_t stream)
{
    return gpu_update_result_buf_k2k(param, stream);
}



// done
void gpu_calc_prefix_sum(GPUEngineParam& param,
                     cudaStream_t stream)
{
    thrust::device_ptr<int> d_in_ptr(param.gpu.d_edge_size_list);
    thrust::device_ptr<int> d_out_ptr(param.gpu.d_prefix_sum_list);
    thrust::inclusive_scan(thrust::cuda::par.on(stream), d_in_ptr, d_in_ptr + param.query.row_num, d_out_ptr);
}


// Calculate destination server for each records in the result buffer
// TODO
__global__
void k_hash_tuples_to_server(sid_t *result_table,
                                  int *server_id_list,
                                  int var2col,
                                  int col_num,
                                  int num_sub_request,
                                  int query_size)
{
    int row_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (row_idx < query_size) {
        // server_id_list[row_idx] =  result_table[row_idx * col_num + (-start - 1)] % num_sub_request;
        server_id_list[row_idx] =  result_table[row_idx * col_num + var2col] % num_sub_request;
    }
}

// done
void hash_dispatched_server_id(sid_t *result_table,
                                  int *server_id_list,
                                  int var2col,
                                  int col_num,
                                  int num_sub_request,
                                  int query_size,
                                  cudaStream_t stream)
{
    k_hash_tuples_to_server<<<WUKONG_GET_BLOCKS(query_size),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(result_table,
                                  server_id_list,
                                  var2col,
                                  col_num,
                                  num_sub_request,
                                  query_size);
}

__global__
void k_history_dispatch(sid_t *result_table,
                        int* position_list,
                        int* server_id_list,
                        int* server_sum_list,
                        int start,
                        int col_num,
                        int num_sub_request,
                        int query_size)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if(index<query_size) {
        int server_id =server_id_list[index];
        position_list[index] = atomicAdd(&server_sum_list[server_id],1);
    }

}

// done
void history_dispatch(sid_t *result_table,
                        int* position_list,
                        int* server_id_list,
                        int* server_sum_list,
                        int start,
                        int col_num,
                        int num_sub_request,
                        int query_size,
                        cudaStream_t stream)
{
    k_history_dispatch<<<WUKONG_GET_BLOCKS(query_size),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(result_table,
                                               position_list,
                                               server_id_list,
                                               server_sum_list,
                                               start,
                                               col_num,
                                               num_sub_request,
                                               query_size);
}


// Put the partitioned result buffer to output result buf (via different offset)
// TODO
__global__
void k_split_result_buf(sid_t *d_in_result_buf,
                                  sid_t *d_out_result_buf,
                                  int *d_position_list,
                                  int *server_id_list,
                                  int *sub_table_hdr_list,
                                  int column_num,
                                  int num_sub_request,
                                  int query_size)
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < query_size) {
        int dst_sid = server_id_list[index];
        int mapped_index = sub_table_hdr_list[dst_sid] + d_position_list[index];
        for (int c = 0; c < column_num; c++) {
            d_out_result_buf[column_num * mapped_index + c] = d_in_result_buf[column_num * index + c];
        }
    }
}

// done
void gpu_split_result_buf(GPUEngineParam &param, int num_servers, cudaStream_t stream)
{
    // bowrrow other buffers for temporary use
    int *d_position_list = (int*) param.gpu.d_slot_id_list;
    int *d_server_id_list = param.gpu.d_prefix_sum_list;
    int *d_server_sum_list = param.gpu.d_edge_size_list;

    k_split_result_buf<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS, 0, stream>>>(
            param.gpu.d_in_rbuf,
            param.gpu.d_out_rbuf,
            d_position_list,
            d_server_id_list,
            d_server_sum_list,
            param.query.col_num, //column_num,
            num_servers, // num_sub_request,
            param.query.row_num);//query_size);
}


// done
void gpu_shuffle_result_buf(GPUEngineParam& param, int num_jobs, vector<int>& buf_sizes,
        vector<int>& buf_heads, cudaStream_t stream)
{
    // bowrrow other buffers for temporary use
    int *d_position_list = (int*) param.gpu.d_slot_id_list;
    int *d_server_id_list = param.gpu.d_prefix_sum_list;
    int *d_server_sum_list = param.gpu.d_edge_size_list;

    // int num_jobs = buf_sizes.size();

    // calculate destination server for each record
    hash_dispatched_server_id(param.gpu.d_in_rbuf,
                                  d_server_id_list,
                                  param.query.var2col_start, //param.query.start_vid,
                                  param.query.col_num,
                                  num_jobs,
                                  param.query.row_num,
                                  stream);

    history_dispatch(param.gpu.d_in_rbuf,
                     d_position_list,
                         d_server_id_list,
                         d_server_sum_list,
                         param.query.var2col_start, //param.query.start_vid,
                         param.query.col_num,
                         num_jobs,
                         param.query.row_num,
                         stream);

    CUDA_ASSERT(cudaMemcpyAsync(&buf_sizes[0],
                                  d_server_sum_list,
                                  sizeof(int) * num_jobs,
                                  cudaMemcpyDeviceToHost,
                                  stream));

    CUDA_STREAM_SYNC(stream);

    // calculate exclusive prefix sum for d_server_sum_list
    thrust::device_ptr<int> dptr(d_server_sum_list);
    thrust::exclusive_scan(thrust::cuda::par.on(stream), dptr, dptr + num_jobs, dptr);


    CUDA_STREAM_SYNC(stream);

    CUDA_ASSERT(cudaMemcpyAsync(&buf_heads[0],
                              d_server_sum_list,
                              sizeof(int) * num_jobs,
                              cudaMemcpyDeviceToHost,
                              stream));

}


__global__
void lookup_hashtable_k2c(GPUEngineParam param)
{
    // TODO
    assert(false);

    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index >= param.query.row_num)
        return;

    d_generate_key_list_k2u(index,
            param.gpu.d_in_rbuf,
            param.gpu.d_key_list,
            param.query.var2col_start, // param.query.start_vid,
            param.query.dir,
            param.query.pid,
            param.query.col_num,
            param.query.row_num);

    // get_slot_id_list
    ikey_t empty_key = ikey_t();

    d_get_slot_id_list(index,
            param.gpu.d_vertex_addr,
            param.gpu.d_key_list,
            param.gpu.d_slot_id_list,
            empty_key,
            param.gpu.d_segment_meta,
            param.gpu.d_vertex_mapping,
            param.gpu.vertex_block_sz,
            param.query.row_num);

    // get_edge_list
    d_get_edge_list_k2c(index,
            param.gpu.d_slot_id_list,
            param.gpu.d_vertex_addr,
            param.gpu.d_prefix_sum_list,
            param.gpu.d_edge_size_list,
            param.gpu.d_edge_off_list,
            param.gpu.d_edge_addr,
            param.query.end_vid,
            param.query.segment_edge_start,
            param.gpu.d_edge_mapping,
            param.gpu.edge_block_sz,
            param.query.row_num);

} 




__global__
void lookup_hashtable_k2u(GPUEngineParam param)
{
    // 1D
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index >= param.query.row_num)
        return;


    d_generate_key_list_k2u(index,
            param.gpu.d_in_rbuf,
            param.gpu.d_key_list,
            param.query.var2col_start, // param.query.start_vid,
            param.query.dir,
            param.query.pid,
            param.query.col_num,
            param.query.row_num);

    // get_slot_id_list
    ikey_t empty_key = ikey_t();

    d_get_slot_id_list(index,
            param.gpu.d_vertex_addr,
            param.gpu.d_key_list,
            param.gpu.d_slot_id_list,
            empty_key,
            param.gpu.d_segment_meta,
            param.gpu.d_vertex_mapping,
            param.gpu.vertex_block_sz,
            param.query.row_num);


    // get_edge_list
    d_get_edge_list(index,
            param.gpu.d_slot_id_list,
            param.gpu.d_vertex_addr,
            param.gpu.d_prefix_sum_list,
            param.gpu.d_edge_size_list,
            param.gpu.d_edge_off_list,
            param.query.segment_edge_start,
            param.gpu.d_edge_mapping,
            param.gpu.edge_block_sz,
            param.query.row_num);

}

void gpu_lookup_hashtable_k2c(GPUEngineParam& param, cudaStream_t stream)
{

    assert(param.query.row_num > 0);
    assert(param.query.var2col_start >= 0);

    lookup_hashtable_k2c<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS>>>(param);
}



void gpu_lookup_hashtable_k2u(GPUEngineParam& param, cudaStream_t stream)
{

    assert(param.query.row_num > 0);
    assert(param.query.var2col_start >= 0);

    lookup_hashtable_k2u<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS>>>(param);
}



// done
__global__
void k_update_result_table_k2u(sid_t *result_table,
                                  sid_t *updated_result_table,
                                  int *index_list,
                                  uint64_t *off_list,
                                  edge_t *edge_addr,
                                  uint64_t* edge_headers,
                                  uint64_t pred_edge_shard_size,
                                  int col_num,
                                  int query_size)
{
    // 1D
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if(index<query_size) {

        int edge_num = 0,start=0;
        if(index==0) {
            edge_num = index_list[index];
            start = 0;
        }
        else {
            edge_num = index_list[index] - index_list[index - 1];
            start = (col_num+1) * index_list[index - 1];
        }

        sid_t buff[20];
        for(int c=0;c<col_num;c++){
            buff[c] = result_table[col_num*index+c];
        }

        for(int k=0;k<edge_num;k++){
            // put original columns to table
            for(int c=0;c<col_num;c++){
                updated_result_table[start+k*(col_num+1)+c] = buff[c];
            }
            // put the new column to table
            uint64_t ptr = map_location_on_shards(off_list[index]+k,
                                                  edge_headers,
                                                  pred_edge_shard_size);

            updated_result_table[start+k*(col_num+1)+col_num] = edge_addr[ptr].val;
        }
    }

}

// update_result_table_k2u
int gpu_update_result_buf_k2u(GPUEngineParam& param, cudaStream_t stream)
{

    int table_size = 0;//index_list[query_size-1];
    CUDA_ASSERT( cudaMemcpy(&table_size,
               param.gpu.d_prefix_sum_list + param.query.row_num - 1,
               sizeof(int),
               cudaMemcpyDeviceToHost) );


    k_update_result_table_k2u<<<WUKONG_GET_BLOCKS(param.query.row_num),
        WUKONG_CUDA_NUM_THREADS>>>(
                param.gpu.d_in_rbuf,
                param.gpu.d_out_rbuf,
                param.gpu.d_prefix_sum_list,
                param.gpu.d_offset_list,
                param.gpu.d_edge_addr,
                param.gpu.d_edge_mapping,
                param.gpu.edge_block_sz,
                param.query.col_num,
                param.query.row_num
         );

    // CUDA_ASSERT( cudaStreamSynchronize(stream) );
    return table_size*(param.query.col_num + 1);

}




