#include "Utils.hpp"
#include <sys/mman.h>
#include <iostream>

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

void load_client_data(vector_el_t** centroids, uint8_t** query_vectors, uint32_t** groundtruth) {
    len_t centroids_size = (len_t)N_LISTS * VECTOR_DIM * sizeof(vector_el_t);
    len_t query_vectors_size = (len_t)1E4 * VECTOR_DIM * sizeof(vector_el_t);
    len_t groundtruth_size = (len_t)1E4 * N_RESULTS_GROUNDTRUTH * sizeof(vector_id_t); // ???

    std::string dataset_dir = join(SIFT_OUTPUT_DIR, DATASET);

    std::string centroids_filepath = join(dataset_dir, get_centroids_filename(N_LISTS));
    std::string query_vectors_filepath = SIFT_QUERY_VECTORS_FILEPATH;
    std::string groundtruth_filepath = join(SIFT_GROUNDTRUTH_DIR, "idx_1M.ivecs");

    std::cout << "centroids_filepath: " << centroids_filepath << std::endl;
    std::cout << "query_vectors_filepath: " << query_vectors_filepath << std::endl;
    std::cout << "groundtruth_filepath: " << groundtruth_filepath << std::endl;
    assert(file_exists(centroids_filepath) || !"Cannot find centroids file!");
    assert(file_exists(query_vectors_filepath) || !"Cannot find query vectors file!");
    assert(file_exists(groundtruth_filepath) || !"Cannot find groundtruth file!");

    *centroids = (vector_el_t*) mmap_file(centroids_filepath, centroids_size);
    *query_vectors = (uint8_t*) mmap_file(query_vectors_filepath, query_vectors_size);
    *groundtruth = (uint32_t*) mmap_file(groundtruth_filepath, groundtruth_size);
}

void load_server_data(std::string& vectors_filepath , std::string& vector_ids_filepath, std::string& list_ids_filepath) {
    len_t vectors_size = (len_t)N_ENTRIES * VECTOR_DIM * sizeof(vector_el_t);
    len_t vector_ids_size = (len_t)N_ENTRIES * sizeof(vector_id_t);
    len_t list_ids_size = (len_t)N_ENTRIES * sizeof(list_id_t);

    std::string dataset_dir = join(SIFT_OUTPUT_DIR, DATASET);

    vectors_filepath = join(dataset_dir, get_vectors_filename());
    vector_ids_filepath = join(dataset_dir, get_vector_ids_filename());
    list_ids_filepath = join(dataset_dir, get_list_ids_filename(N_LISTS));

    std::cout << "vectors_filepath: " << vectors_filepath << std::endl;
    std::cout << "vector_ids_filepath: " << vector_ids_filepath << std::endl;
    std::cout << "list_ids_filepath: " << list_ids_filepath << std::endl;
    assert(file_exists(vectors_filepath) || !"Cannot find vectors file!");
    assert(file_exists(vector_ids_filepath) || !"Cannot find vectors ids file!");
    assert(file_exists(list_ids_filepath) || !"Cannot find list ids file!");

    // vectors = (vector_el_t*) mmap_file(vectors_filepath, vectors_size);
    // vector_ids = (vector_id_t*) mmap_file(vector_ids_filepath, vector_ids_size);
    // list_ids = (list_id_t*) mmap_file(list_ids_filepath, list_ids_size);
}

std::vector<float> alloc_query_as_float(uint8_t *query_vector, len_t vector_dim) {
    std::vector<float> single_query_float;
    for (len_t i = 0; i < vector_dim; i++)
    {
        single_query_float.push_back((float)query_vector[i]);
    }

    return single_query_float;
};