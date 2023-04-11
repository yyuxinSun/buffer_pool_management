#ifndef UTILS_ANN_DKVS_H
#define UTILS_ANN_DKVS_H
#include <fstream>
#include <vector>

#include "../include/storage-node/StorageLists.hpp"

using namespace ann_dkvs;

#define SIFT_OUTPUT_DIR "cluster/out"
#define SIFT_GROUNDTRUTH_DIR "cluster/data/bigann/gnd"
#define SIFT_QUERY_VECTORS_FILEPATH "cluster/data/bigann/bigann_query.bvecs"
#define LISTS_FILENAME "lists"
#define VECTORS_FILENAME "vectors"
#define VECTOR_IDS_FILENAME "vector_ids"
#define LIST_IDS_FILENAME "list_ids"
#define CENTROIDS_FILENAME "centroids"
#define FILE_EXT ".bin"

#define TMP_DIR "tests/tmp"

#define DATASET "SIFT1M"

#define N_ENTRIES 1E6
#define N_LISTS 1024
#define VECTOR_DIM 128
#define N_RESULTS 1
#define N_PROBES 32
#define N_RESULTS_GROUNDTRUTH 1000


void* mmap_file(std::string filename, size_t size);

std::string join(const std::string &a, const std::string &b);

bool file_exists(std::string filename);

std::string get_vectors_filename();

std::string get_vector_ids_filename();

std::string get_list_ids_filename(len_t n_lists);

std::string get_centroids_filename(len_t n_lists);

std::string get_lists_filename();

StorageLists get_inverted_lists_object(len_t vector_dim);

void load_client_data(vector_el_t** centroids, uint8_t** query_vectors, uint32_t** groundtruth);

void load_server_data(std::string& vectors_filepath , std::string& vector_ids_filepath, std::string& list_ids_filepath);

std::vector<float> alloc_query_as_float(uint8_t *query_vector, len_t vector_dim);

#endif