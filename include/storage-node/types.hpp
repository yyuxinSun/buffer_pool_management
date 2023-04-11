#pragma once

#include <vector>
#include <cassert>

namespace ann_dkvs
{
  // 1 byte
  typedef unsigned char uint8_t;
  // 4 bytes
  typedef float vector_el_t;
  typedef float distance_t;
  // 8 bytes
  typedef unsigned long len_t;
  typedef unsigned long size_t;
  typedef signed long int64_t;
  typedef int64_t list_id_t;
  typedef int64_t vector_id_t;

  /**
   * My defination.
  */
  typedef int64_t frame_id_t; 
  // typedef int64_t list_id_t;
  // typedef int64_t vector_el_t;
  // typedef int64_t vector_id_t;

  #define DATA_DIMENSION 128
  // #define FRAME_DATA_NUM 760 
  // #define FRAME_DATA_NUM 3000 // One frame in buffer pool stores this number of data. // 4000 for 1000M; 3000 for 100M
  #define FRAME_DATA_SIZE FRAME_DATA_NUM * DATA_DIMENSION // assume 500 vectors with 128 dimensions; 
                                // when it changes, also check the constructor of buffer pool, hash_to_list_size_
  #define INVALID_LIST_ID -1

  #define NUM_LISTS 16384
}