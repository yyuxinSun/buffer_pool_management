#include <vector>
#include <cassert>
#include <iostream>
#include <cstring>

#include "../include/L2Space.hpp"
#include "../include/root-node/RootIndex.hpp"

namespace ann_dkvs
{
  RootIndex::RootIndex(len_t vector_dim, vector_el_t *centroids, len_t n_centroids)
      : vector_dim(vector_dim), centroids(centroids), n_centroids(n_centroids)
  {
    this->centroids = (vector_el_t *)malloc(n_centroids * vector_dim * sizeof(vector_el_t));
    memcpy(this->centroids, centroids, n_centroids * vector_dim * sizeof(vector_el_t));
  }

  RootIndex::~RootIndex()
  {
    free(centroids);
  }

  void RootIndex::add_candidate(const Query *query, const CentroidsResult &result, centroids_heap_t &candidates)
  {
    if (candidates.size() < query->get_n_probe())
    {
      candidates.push(result);
    }
    else if (result < candidates.top())
    {
      candidates.pop();
      candidates.push(result);
    }
  }

  void RootIndex::allocate_list_ids(Query *query, centroids_heap_t *nearest_centroids)
  {
    for (size_t query_id = 0; query_id < query->get_n_probe(); query_id++)
    {
      size_t insertion_index = query->get_n_probe() - query_id - 1;
      list_id_t list_id = nearest_centroids->top().list_id;
      query->set_list_to_probe(insertion_index, list_id);
      nearest_centroids->pop();
    }
  }

  void RootIndex::preassign_query(Query *query)
  {
    centroids_heap_t candidates;
    distance_func_t distance_func = L2Space(vector_dim).get_distance_func();

    for (list_id_t list_id = 0; list_id < (list_id_t)n_centroids; list_id++)
    {
      vector_el_t *centroid = &centroids[list_id * vector_dim];
      float distance = distance_func(centroid, query->get_query_vector(), &vector_dim);
      const CentroidsResult result = {.distance = distance, .list_id = list_id};
      add_candidate(query, result, candidates);
    }
    allocate_list_ids(query, &candidates);
  }

  void RootIndex::batch_preassign_queries(QueryBatch queries)
  {
#if PMODE != 0
#pragma omp parallel for schedule(runtime)
#endif
    for (len_t i = 0; i < queries.size(); i++)
    {
      preassign_query(queries[i]);
    }
  }
}
