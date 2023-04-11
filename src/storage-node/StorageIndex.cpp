#include <iostream>

#include "storage-node/StorageIndex.hpp"
#include "L2Space.hpp"

namespace ann_dkvs
{

  QueryResults StorageIndex::extract_results(heap_t &candidates) const
  {
    len_t n_results = candidates.size();
    QueryResults results(n_results);
    for (size_t i = 0; i < n_results; i++)
    {
      results[n_results - i - 1] = candidates.top();
      candidates.pop();
    }
    return results;
  }

  void StorageIndex::add_candidate(const Query *query, const QueryResult &result, heap_t &candidates) const
  {
    if (candidates.size() < query->get_n_results())
    {
      candidates.push(result);
    }
    else if (result < candidates.top())
    {
      candidates.pop();
      candidates.push(result);
    }
  }

  void StorageIndex::search_preassigned_list(
      const Query *query,
      const list_id_t list_id,
      heap_t &candidates) const
  {
    const vector_el_t *vectors = lists->get_vectors(list_id);
    const vector_id_t *ids = lists->get_ids(list_id);
    size_t list_size = lists->get_list_length(list_id);
    size_t vector_dim = lists->get_vector_dim();
    for (size_t j = 0; j < list_size; j++)
    {
      const vector_el_t *vector = &vectors[j * vector_dim];
      float distance = distance_func(vector, query->get_query_vector(), &vector_dim);
      const vector_id_t vector_id = ids[j];
      QueryResult result = {distance, vector_id};
      add_candidate(query, result, candidates);
    }
  }

  StorageIndex::StorageIndex(const StorageLists *lists)
      : lists(lists), distance_func(L2Space(lists->get_vector_dim()).get_distance_func())
  {
  }

  QueryResults StorageIndex::search_preassigned(const Query *query) const
  {
    heap_t candidates;
    for (len_t i = 0; i < query->get_n_probe(); i++)
    {
      list_id_t list_id = query->get_list_to_probe(i);
      search_preassigned_list(query, list_id, candidates);
    }
    return extract_results(candidates);
  }

  QueryListPairs StorageIndex::get_work_items(const QueryBatch &queries) const
  {
    QueryListPairs work_items;
    for (len_t i = 0; i < queries.size(); i++)
    {
      for (len_t j = 0; j < queries[i]->get_n_probe(); j++)
      {
        list_id_t list_id = queries[i]->get_list_to_probe(j);
        work_items.push_back({i, list_id});
      }
    }
    return work_items;
  }

  QueryResultsBatch StorageIndex::batch_search_preassigned(const QueryBatch &queries) const
  {
    QueryResultsBatch results(queries.size());

#if PMODE == 0 || PMODE == 1
#if PMODE != 0
#pragma omp parallel for schedule(runtime)
#endif
    for (len_t i = 0; i < queries.size(); i++)
    {
      results[i] = search_preassigned(queries[i]);
    }
#elif PMODE == 2
    std::vector<heap_t> candidate_lists(queries.size());

    QueryListPairs work_items = get_work_items(queries);

#pragma omp parallel for schedule(runtime)
    for (len_t i = 0; i < work_items.size(); i++)
    {
      len_t query_index = work_items[i].first;
      const Query *query = queries[query_index];
      list_id_t list_id = work_items[i].second;
      heap_t local_candidates;
      search_preassigned_list(query, list_id, local_candidates);
#pragma omp critical
      {
        while (local_candidates.size() > 0)
        {
          QueryResult result = local_candidates.top();
          local_candidates.pop();
          add_candidate(query, result, candidate_lists[query_index]);
        }
      }
    }
#pragma omp single
    for (len_t j = 0; j < queries.size(); j++)
    {
      results[j] = extract_results(candidate_lists[j]);
    }
#endif
    return results;
  }


  /** Adding buffer pool management. */

  void StorageIndex::search_preassigned_list_bpm(
      const Query *query,
      const list_id_t list_id,
      heap_t &candidates,
      BufferPoolManager* bpm) const
  {
    // const vector_el_t *vectors_mm = lists->get_vectors(list_id);
    // const vector_id_t *ids_mm = lists->get_ids(list_id);




    std::vector<frame_id_t> frame_lists = bpm->FetchListPages(list_id);
    size_t frame_list_size = frame_lists.size();

    size_t list_size = lists->get_list_length(list_id);
    size_t vector_dim = lists->get_vector_dim();

    size_t read_size = 0;
    int full = list_size % FRAME_DATA_NUM;

    int k = 0;

    for (size_t i = 0; i < frame_list_size; i++) {
      // std::cout << "load new" << std::endl;
      frame_id_t frame_id = frame_lists[i];
      const vector_el_t* vectors = (vector_el_t*) bpm->GetPageVectors(frame_id);
      const vector_id_t* ids = (vector_id_t*) bpm->GetPageIDs(frame_id);

      if (i != frame_list_size - 1 || full == 0) {
        // std::cout << "list_size: " << list_size << std::endl;
        for (int j = 0; j < FRAME_DATA_NUM; j++) {
          const vector_el_t *vector = &vectors[j * vector_dim];


          // const vector_el_t *vector_mm = &vectors_mm[k * vector_dim];
          // std::cout << "bpm: ";
          // for (int m = 0; m < vector_dim; m++) {
          //   vector_el_t out = vector[m];
          //   std::cout << out << " ";
          // }
          // std::cout << std::endl;
          // std::cout << "mmap: ";
          // for (int m = 0; m < vector_dim; m++) {
          //   vector_el_t out = vector_mm[m];
          //   std::cout << out << " ";
          // }
          // std::cout << std::endl;
          // std::cout << "bpm: " << ids[j] << std::endl;
          // std::cout << "mmap: " << ids_mm[k] << std::endl;
          // std::cout << "non last k: " << k << std::endl;
          // std::cout << "non last j: " << j << std::endl;
          // assert(ids[j] == ids_mm[k] || !"non-equal!");
          // k++;

          
          float distance = distance_func(vector, query->get_query_vector(), &vector_dim);
          const vector_id_t vector_id = ids[j];
          QueryResult result = {distance, vector_id};
          add_candidate(query, result, candidates);

          read_size++;
        }
      } else {
        // std::cout << "list_size: " << list_size << std::endl;
        int num_in_last_frame = list_size % FRAME_DATA_NUM;
        for (int j = 0; j < num_in_last_frame; j++) {
          const vector_el_t *vector = &vectors[j * vector_dim];


          // const vector_el_t *vector_mm = &vectors_mm[k * vector_dim];
          // std::cout << "bpm: ";
          // for (int m = 0; m < vector_dim; m++) {
          //   vector_el_t out = vector[m];
          //   std::cout << out << " ";
          // }
          // std::cout << std::endl;
          // std::cout << "mmap: ";
          // for (int m = 0; m < vector_dim; m++) {
          //   vector_el_t out = vector_mm[m];
          //   std::cout << out << " ";
          // }
          // std::cout << std::endl;
          // std::cout << "bpm: " << ids[j] << std::endl;
          // std::cout << "mmap: " << ids_mm[k] << std::endl;
          // std::cout << "last k: " << k << std::endl;
          // std::cout << "last j: " << j << std::endl;
          // assert(ids[j] == ids_mm[k] || !"non-equal!");
          // assert("Success!");
          // k++;


          float distance = distance_func(vector, query->get_query_vector(), &vector_dim);
          const vector_id_t vector_id = ids[j];
          QueryResult result = {distance, vector_id};
          add_candidate(query, result, candidates);

          read_size++;
        }
      }

    }
    
    assert(read_size == list_size || !"Error size read from function search_preassigned_list_bpm()!");

    bpm->UnPinListPages(list_id);
  }

  QueryResults StorageIndex::search_preassigned_bpm(const Query *query, BufferPoolManager* bpm) const
  {
    heap_t candidates;
    for (len_t i = 0; i < query->get_n_probe(); i++)
    {
      list_id_t list_id = query->get_list_to_probe(i);
      search_preassigned_list_bpm(query, list_id, candidates, bpm);
    }
    return extract_results(candidates);
  }

  QueryResultsBatch StorageIndex::batch_search_preassigned_bpm(const QueryBatch &queries, BufferPoolManager* bpm) const
  {
    QueryResultsBatch results(queries.size());

#if PMODE == 0 || PMODE == 1
#if PMODE != 0
#pragma omp parallel for schedule(runtime)
#endif
    for (len_t i = 0; i < queries.size(); i++)
    {
      results[i] = search_preassigned_bpm(queries[i], bpm);
    }
#elif PMODE == 2
    std::vector<heap_t> candidate_lists(queries.size());

    QueryListPairs work_items = get_work_items(queries);

#pragma omp parallel for schedule(runtime)
    for (len_t i = 0; i < work_items.size(); i++)
    {
      len_t query_index = work_items[i].first;
      const Query *query = queries[query_index];
      list_id_t list_id = work_items[i].second;
      heap_t local_candidates;
      search_preassigned_list_bpm(query, list_id, local_candidates, bpm);
#pragma omp critical
      {
        while (local_candidates.size() > 0)
        {
          QueryResult result = local_candidates.top();
          local_candidates.pop();
          add_candidate(query, result, candidate_lists[query_index]);
        }
      }
    }
#pragma omp single
    for (len_t j = 0; j < queries.size(); j++)
    {
      results[j] = extract_results(candidate_lists[j]);
    }
#endif
    return results;
  }


}