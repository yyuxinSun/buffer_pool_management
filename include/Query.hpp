#pragma once

#include <vector>

#include "storage-node/types.hpp"

namespace ann_dkvs
{
  class Query
  {
  private:
    vector_el_t *query_vector;
    list_id_t *list_to_probe;
    const len_t n_results;
    const len_t n_probes;
    bool free_list_to_probe = false;

  public:
    Query(vector_el_t *query_vector, const len_t n_results, const len_t n_probes);
    Query(vector_el_t *query_vector, list_id_t *list_to_probe, const len_t n_results, const len_t n_probes);
    vector_el_t *get_query_vector() const;
    list_id_t get_list_to_probe(const len_t i) const;
    len_t get_n_results() const;
    len_t get_n_probe() const;
    void set_list_to_probe(const len_t offset, const list_id_t list_id) const;
    ~Query();
  };

  typedef std::vector<Query *> QueryBatch;

  struct QueryResult
  {
    distance_t distance;
    vector_id_t vector_id;
    friend bool operator<(const QueryResult &a, const QueryResult &b)
    {
      bool closer = a.distance < b.distance ||
                    (a.distance == b.distance && a.vector_id < b.vector_id);
      return closer;
    }
  };

  typedef std::vector<QueryResult> QueryResults;
  typedef std::vector<QueryResults> QueryResultsBatch;
} // namespace ann_dkvs