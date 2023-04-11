#pragma once

#include <string>
#include <queue>

#include "StorageLists.hpp"
#include "../L2Space.hpp"
#include "../Query.hpp"

#include "../buffer_management/BufferPoolManager.hpp"

namespace ann_dkvs
{
  /**
   * @brief A class that implements a comparison function used for a max heap.
   */
  class VectorDistanceIdMaxHeapCompare
  {
  public:
    bool operator()(const QueryResult &parent, const QueryResult &child) const
    {
      bool swapParentWithChild = parent < child;
      return swapParentWithChild;
    }
  };
  /**
   * Data structure used to store the results of a query.
   */
  typedef std::priority_queue<QueryResult, std::vector<QueryResult>, VectorDistanceIdMaxHeapCompare> heap_t;

  /**
   * Internal data structure representing a work iterm for a thread.
   */
  typedef std::pair<len_t, list_id_t> QueryListPair;

  /**
   * Internal data structure representing a list of work items
   * which are distributed among the available threads.
   */
  typedef std::vector<QueryListPair> QueryListPairs;

  class StorageIndex
  {

  private:
    /**
     * Pointer to a storage lists object
     * used to access the data.
     */
    const StorageLists *lists;

    /**
     * Distance function used to compute the distance
     * between query vector and the vectors within a list.
     */
    const distance_func_t distance_func;

    /**
     * Converts a heap of results into a QueryResults object,
     * i.e. a vector of QueryResult objects.
     *
     * @param candidates A heap of query results.
     * @return A vector of query results.
     */
    QueryResults extract_results(heap_t &candidates) const;

    /**
     * Searches a single list for the nearest neighbors of a query.
     *
     * @param query A pointer to a query object.
     * @param list_id The id of the list to search.
     * @param candidates A reference to a heap of query results used to store the query results.
     */
    void search_preassigned_list(
        const Query *query,
        const list_id_t list_id,
        heap_t &candidates) const;


    void search_preassigned_list_bpm(
        const Query *query,
        const list_id_t list_id,
        heap_t &candidates, BufferPoolManager* bpm) const;

    /**
     * Creates a list of work items for a batch of queries.
     * Each work item is a pair of a query id and a list id.
     * The work items are distributed among the available threads.
     *
     * @param queries A batch of queries.
     * @return A vector of work items.
     */
    QueryListPairs get_work_items(const QueryBatch &queries) const;

    /**
     * Adds a given query result to the heap of candidate results.
     *
     * Only updates the heap if the candidate result is closer
     * than the furthest result in the heap or if the heap is not full.
     *
     * @param query A pointer to a query object.
     * @param candidate A query result,
     *                   i.e. a pair of a distance and a vector id.
     * @param candidates A reference to a heap of query results used to store the query results.
     */
    void add_candidate(const Query *query, const QueryResult &candidate, heap_t &candidates) const;

  public:
    /**
     * Creates a new storage index object.
     *
     * @param lists A pointer to a storage lists object
     */
    StorageIndex(const StorageLists *lists);

    /**
     * Searches all lists of a query selected for probing
     * to find the query's nearest neighbors.
     *
     * @param query A pointer to a query object.
     * @return A vector of query results.
     */
    QueryResults search_preassigned(const Query *query) const;

    /**
     * Searches all lists of a batch of queries selected for probing
     * to find the queries' nearest neighbors.
     *
     * @param queries A batch of queries, i.e. a vector of query objects.
     * @return A batch of query results,
     *          i.e. a vector of vectors of query results.
     */
    QueryResultsBatch batch_search_preassigned(const QueryBatch &queries) const;


    QueryResults search_preassigned_bpm(const Query *query, BufferPoolManager* bpm) const;
    QueryResultsBatch batch_search_preassigned_bpm(const QueryBatch &queries, BufferPoolManager* bpm) const;
  };
}