#pragma once

#include <vector>
#include <queue>

#include "../storage-node/types.hpp"
#include "../Query.hpp"

namespace ann_dkvs
{
  /**
   * @brief A struct that represents a centroid candidate containing
   * the distance to the query vector and the list id.
   */
  struct CentroidsResult
  {
    distance_t distance;
    list_id_t list_id;
    friend bool operator<(const CentroidsResult &a, const CentroidsResult &b)
    {
      bool closer = a.distance < b.distance ||
                    (a.distance == b.distance && a.list_id < b.list_id);
      return closer;
    }
  };

  /**
   * @brief A class that implements a comparison function used for a max heap.
   */
  class CentroidsDistanceIdMaxHeapCompare
  {
  public:
    bool operator()(const CentroidsResult &parent, const CentroidsResult &child)
    {
      bool swapParentWithChild = parent < child;
      return swapParentWithChild;
    }
  };

  /**
   * Data structure used to store the centroid candidates.
   */
  typedef std::priority_queue<CentroidsResult, std::vector<CentroidsResult>, CentroidsDistanceIdMaxHeapCompare> centroids_heap_t;

  class RootIndex
  {
  private:
    /**
     * Dimension of the centroid vectors.
     */
    len_t vector_dim;

    /**
     * Pointer to the centroid vectors.
     */
    vector_el_t *centroids;

    /**
     * Number of centroid vectors.
     */
    len_t n_centroids;

    /**
     * Given a query and the results of the nearest centroid search,
     * this function sets the lists to be searched for the query.
     *
     * @param query A pointer to the query object.
     * @param nearest_centroids A heap of the nearest centroid candidates.
     */
    void allocate_list_ids(Query *query, centroids_heap_t *nearest_centroids);

    /**
     * Given a query and a centroid candidate, this function adds the candidate
     * to the heap of candidates.
     *
     * The heap is only updated if the candidate is closer than the furthest
     * candidate in the heap or if the heap is not full.
     *
     * @param query A pointer to the query object.
     * @param candidate A centroid candidate.
     * @param candidates A heap of the nearest centroid candidates.
     */
    void add_candidate(const Query *query, const CentroidsResult &candidate, centroids_heap_t &candidates);

  public:
    /**
     * Creates a new root index object.
     *
     * Allocates memory on the heap for the centroid vectors.
     *
     * @param vector_dim Dimension of the centroid vectors.
     * @param centroids Pointer to the centroid vectors.
     * @param n_centroids Number of centroid vectors.
     */
    RootIndex(len_t vector_dim, vector_el_t *centroids, len_t n_centroids);

    /**
     * Destroys the root index object.
     *
     * Frees the memory allocated for the centroid vectors.
     */
    ~RootIndex();

    /**
     * Finds the nearest centroids of the query
     * and sets the list ids to be searched for the query.
     *
     * @param query A pointer to the query object.
     */
    void preassign_query(Query *query);

    /**
     * Finds the nearest centroids of a list of queries
     * and sets the list ids to be searched.
     *
     * @param queries A query batch object.
     */
    void batch_preassign_queries(QueryBatch queries);
  };
} // namespace ann_dkvs