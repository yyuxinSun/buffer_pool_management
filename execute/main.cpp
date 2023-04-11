#include <stdio.h>
#include <sys/mman.h>
#include <string>
#include <fstream>
#include <iostream>
#include <chrono>

#include <boost/serialization/vector.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

// #include "../include/storage-node/StorageLists.hpp"
#include "../include/buffer_management/BufferPoolManager.hpp"
#include "../include/storage-node/StorageIndex.hpp"
#include "../include/root-node/RootIndex.hpp"
#include "../include/Query.hpp"

#define SIFT_OUTPUT_DIR "cluster/out"
#define SIFT_GROUNDTRUTH_DIR "cluster/data/bigann/gnd"
#define SIFT_QUERY_VECTORS_FILEPATH "cluster/data/bigann/bigann_query.bvecs"
#define LISTS_FILENAME "lists_100M_large"
#define VECTORS_FILENAME "vectors"
#define VECTOR_IDS_FILENAME "vector_ids"
#define LIST_IDS_FILENAME "list_ids"
#define CENTROIDS_FILENAME "centroids"
#define FILE_EXT ".bin"

#define TMP_DIR "tests/tmp"

#define DATASET "SIFT100M"

#define N_ENTRIES 1E8
#define N_LISTS 131072
#define VECTOR_DIM 128
#define N_RESULTS 1
#define N_PROBES 32
#define N_RESULTS_GROUNDTRUTH 1000

#define OUTPUT_ARVHICE "output/list_archive_100M_large"


using namespace ann_dkvs;

void* mmap_file(std::string filename, size_t size) {
    FILE* file = fopen(filename.c_str(), "r");
    void* data = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fileno(file), 0);
    fclose(file);
    return data;
}

std::string join(const std::string &a, const std::string &b)
{
    return a + "/" + b;
}

bool file_exists(std::string filename)
{
    std::ifstream f(filename.c_str());
    if (!f.is_open())
    {
        return false;
    }
    f.close();
    return true;
}

std::string get_vectors_filename()
{
    std::string filename = VECTORS_FILENAME;
    std::string file_ext = FILE_EXT;
    std::string result = filename;
    return result + file_ext;
}

std::string get_vector_ids_filename()
{
    std::string filename = VECTOR_IDS_FILENAME;
    std::string file_ext = FILE_EXT;
    std::string result = filename;
    return result + file_ext;
}

std::string get_list_ids_filename(len_t n_lists)
{
    std::string filename = LIST_IDS_FILENAME;
    std::string separator = "_";
    std::string file_ext = FILE_EXT;
    std::string result = filename + separator + std::to_string(n_lists);
    return result + file_ext;
}

std::string get_centroids_filename(len_t n_lists)
{
    std::string filename = CENTROIDS_FILENAME;
    std::string seperator = "_";
    std::string file_ext = FILE_EXT;
    return filename + seperator + std::to_string(n_lists) + file_ext;
}

std::string get_lists_filename()
{
    std::string filename = LISTS_FILENAME;
    std::string file_ext = FILE_EXT;
    return filename + file_ext;
}

StorageLists get_inverted_lists_object(len_t vector_dim)
{
    std::string file = join(TMP_DIR, get_lists_filename());
    remove(file.c_str());
    StorageLists lists(vector_dim, file);
    return lists;
}

auto alloc_query_as_vector_el = [](uint8_t *query_vector, len_t vector_dim)
{
    vector_el_t *query_vector_float = (vector_el_t *)malloc(vector_dim * sizeof(vector_el_t));
    for (len_t i = 0; i < vector_dim; i++)
    {
        query_vector_float[i] = (vector_el_t)query_vector[i];
    }
    return query_vector_float;
};

auto prepare_query = [](uint8_t *query_vectors, 
                        len_t query_id, 
                        len_t vector_dim, 
                        len_t n_results, 
                        len_t n_probes)
{
    uint8_t *query_bytes = &query_vectors[query_id * (vector_dim + 4) + 4];
    vector_el_t *query_vector = alloc_query_as_vector_el(query_bytes, vector_dim);
    Query *query = new Query(query_vector, n_results, n_probes);
    return query;
};

auto prepare_queries = [](uint8_t *query_vectors, len_t n_query_vectors, len_t vector_dim, len_t n_results, len_t n_probes)
{
  QueryBatch queries;
  int k = 0;

  for (len_t query_id = 0; query_id < n_query_vectors; query_id++)
  {
    Query *query = prepare_query(query_vectors, query_id, vector_dim, n_results, n_probes);
    queries.push_back(query);

    for (int i = 0; i < 2; i++) {
        Query *hot_query = prepare_query(query_vectors, (len_t) k % 30, vector_dim, n_results, n_probes);
        queries.push_back(hot_query);
        k++;
    }
  }
  return queries;
};

auto get_recall_at_r = [](const QueryResultsBatch &result_batch, const uint32_t *groundtruth, len_t n_query_vectors, len_t n_results_groundtruth, len_t n_results, len_t r)
{
    assert(r <= n_results || !"r should be smaller than n_results!");
    len_t n_correct = 0;

    int k = 0;
    for (len_t query_id = 0; query_id < n_query_vectors * 3; query_id++)
    {
        const QueryResults results = result_batch[query_id];

        vector_id_t groundtruth_first_nearest_neighbor;
        /** original queries */
        if (query_id % 3 == 0) {
            groundtruth_first_nearest_neighbor = groundtruth[(query_id / 3) * (n_results_groundtruth + 1) + 1];
        }

        /** hot queries */
        if (query_id % 3) {
            groundtruth_first_nearest_neighbor = groundtruth[(k % 30) * (n_results_groundtruth + 1) + 1];
            k++;
        }

        for (len_t result_id = 0; result_id < r; result_id++)
        {
            const vector_id_t id = results[result_id].vector_id;
            if (id == groundtruth_first_nearest_neighbor)
            {
                n_correct++;
                break;
            }
        }
    }
    return (float)n_correct / (n_query_vectors * 3);
};

auto free_query = [](Query *query)
{
    free(query->get_query_vector());
    delete query;
};

auto free_queries = [](QueryBatch queries)
{
    for (len_t query_id = 0; query_id < queries.size(); query_id++)
    {
    free_query(queries[query_id]);
    }
};

int main() {
    len_t vectors_size = (len_t)N_ENTRIES * VECTOR_DIM * sizeof(vector_el_t);
    len_t vector_ids_size = (len_t)N_ENTRIES * sizeof(vector_id_t);
    len_t list_ids_size = (len_t)N_ENTRIES * sizeof(list_id_t);
    len_t centroids_size = (len_t)N_LISTS * VECTOR_DIM * sizeof(vector_el_t);
    len_t query_vectors_size = (len_t)1E4 * VECTOR_DIM * sizeof(vector_el_t);
    len_t groundtruth_size = (len_t)1E4 * N_RESULTS_GROUNDTRUTH * sizeof(vector_id_t); // ???

    std::string dataset_dir = join(SIFT_OUTPUT_DIR, DATASET);

    std::string vectors_filepath = join(dataset_dir, get_vectors_filename());
    std::string vector_ids_filepath = join(dataset_dir, get_vector_ids_filename());
    std::string list_ids_filepath = join(dataset_dir, get_list_ids_filename(N_LISTS));
    std::string centroids_filepath = join(dataset_dir, get_centroids_filename(N_LISTS));
    std::string query_vectors_filepath = SIFT_QUERY_VECTORS_FILEPATH;
    std::string groundtruth_filepath = join(SIFT_GROUNDTRUTH_DIR, "idx_100M.ivecs");

    assert(file_exists(vectors_filepath) || !"Cannot find vectors file!");
    assert(file_exists(vector_ids_filepath) || !"Cannot find vectors ids file!");
    assert(file_exists(list_ids_filepath) || !"Cannot find list ids file!");
    assert(file_exists(centroids_filepath) || !"Cannot find centroids file!");
    assert(file_exists(query_vectors_filepath) || !"Cannot find query vectors file!");
    assert(file_exists(groundtruth_filepath) || !"Cannot find groundtruth file!");

    vector_el_t* vectors = (vector_el_t*) mmap_file(vectors_filepath, vectors_size);
    vector_id_t* vector_ids = (vector_id_t*) mmap_file(vector_ids_filepath, vector_ids_size);
    list_id_t* list_ids = (list_id_t*) mmap_file(list_ids_filepath, list_ids_size);
    vector_el_t* centroids = (vector_el_t*) mmap_file(centroids_filepath, centroids_size);
    uint8_t* query_vectors = (uint8_t*) mmap_file(query_vectors_filepath, query_vectors_size);
    uint32_t* groundtruth = (uint32_t*) mmap_file(groundtruth_filepath, groundtruth_size);

    RootIndex root_index(VECTOR_DIM, centroids, N_LISTS);

    /** Initialize lists. */
    StorageLists lists;
    if (!file_exists(OUTPUT_ARVHICE)) {
        std::cout << "new insertion" << std::endl;
        lists = get_inverted_lists_object(VECTOR_DIM);
        lists.bulk_insert_entries(vectors_filepath, vector_ids_filepath, list_ids_filepath, N_ENTRIES);

        /**
        * Save data to archive.
        */
        std::cout << "save to archive" << std::endl;
        std::ofstream ofs(OUTPUT_ARVHICE);
        boost::archive::text_oarchive oa(ofs);
        oa << lists;
    } else {
        std::ifstream ifs(OUTPUT_ARVHICE);
        boost::archive::text_iarchive ia(ifs);
        ia >> lists;

        FILE *f = fopen("tests/tmp/lists_100M_large.bin", "r+");
        if (f == nullptr)
        {
        throw std::runtime_error("Could not open file tests/tmp/lists_100M_large.bin");
        }
        lists.base_ptr = (uint8_t *)mmap(
            nullptr,
            lists.total_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fileno(f),
            0);
        if (lists.base_ptr == MAP_FAILED)
        {
        throw std::runtime_error("Could not mmap file tests/tmp/lists_100M_large.bin");
        }
        fclose(f);
    }
    
    StorageIndex storage_index(&lists);


    QueryBatch queries = prepare_queries(query_vectors, 1E2, VECTOR_DIM, N_RESULTS, N_PROBES);
    std::cout << "Finished preparing queries." << std::endl;


    // BufferPoolManager* bpm = new BufferPoolManager(600000, &lists, "tests/tmp/lists_100M_large.bin");
    std::cout << "Finished preparing buffer pool." << std::endl;
    root_index.batch_preassign_queries(queries);

    auto start_point = std::chrono::system_clock::now();
    QueryResultsBatch results = storage_index.batch_search_preassigned(queries);
    // QueryResultsBatch results = storage_index.batch_search_preassigned_bpm(queries, bpm);

    /** bpm */
    // int query_size = queries.size();
    // QueryResultsBatch results;
    // for (int i = 0; i < query_size; i++) {
    //     QueryBatch per_query;
    //     per_query.push_back(queries[i]);
    //     QueryResultsBatch per_result = storage_index.batch_search_preassigned(per_query);
    //     std::cout << "2" << std::endl;
    //     QueryResultsBatch per_result = storage_index.batch_search_preassigned_bpm(per_query, bpm);
    //     results.push_back(per_result[0]);
    // }

    auto end_point = std::chrono::system_clock::now();

    std::cout << "3" << std::endl;

    float recall_at_1 = get_recall_at_r(results, groundtruth, 1E2, N_RESULTS_GROUNDTRUTH, N_RESULTS, 1);
    std::cout << "Recall@1: " << recall_at_1 << std::endl;

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_point - start_point);
    std::cout << "duration: " << duration.count() << std::endl;

    // std::cout << "cache hit: " << bpm->hit / (float) bpm->total << std::endl;

    free_queries(queries);

    munmap(vectors, vectors_size);
    munmap(vector_ids, vector_ids_size);
    munmap(list_ids, list_ids_size);
    munmap(centroids, centroids_size);
    munmap(query_vectors, query_vectors_size);
    munmap(groundtruth, groundtruth_size);

    return 0;
}