#pragma once

#include <unordered_map>
#include <vector>
#include <string>

#include <boost/serialization/vector.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include "types.hpp"

#define PROT_READ 0x1
#define PROT_WRITE 0x2
#define FLAG_READ "r"
#define FLAG_READ_WRITE "r+"

#ifndef MIN_TOTAL_SIZE_BYTES
#define MIN_TOTAL_SIZE_BYTES 32
#endif
#ifndef MIN_N_ENTRIES_PER_LIST
#define MIN_N_ENTRIES_PER_LIST 1
#endif
#ifndef MAX_BUFFER_SIZE
#define MAX_BUFFER_SIZE 100000
#endif

namespace ann_dkvs
{
  class StorageLists
  {
  
  public:
    /**
     * Contains meta-data about an inverted list in the memory-mapped file.
     *
     * - offset: offset in bytes relative to base_ptr
     * - allocated_entries: maximum number of entries
     * that the list can hold without having to be extended.
     * - used_entries: number of entries currently
     *  in use and containing valid data (vectors and vector ids)
     */
    struct InvertedList
    { 
      template <class Archive>
      void serialize(Archive & ar, const unsigned int version){
        ar & offset;
        ar & allocated_entries;
        ar & used_entries;
      }
      size_t offset;
      len_t allocated_entries;
      len_t used_entries;
    };

    /**
     * Contains meta-data about a free slot of memory in the memory-mapped file
     * which can be used to allocate new inverted lists or extend existing ones.
     *
     * - offset: offset in bytes relative to base_ptr
     * - size: size in bytes of the free slot
     */
    struct Slot
    {
      template <class Archive>
      void serialize(Archive & ar, const unsigned int version){
        ar & offset;
        ar & size;
      }
      size_t offset;
      size_t size;
    };

    /**
     * A type that represents a map from list ids to inverted lists.
     */
    typedef std::unordered_map<list_id_t, InvertedList> list_id_list_map_t;

    /**
     * A type that represents a map from list ids to the number of entries
     * to be allocated. Used to pre-allocate inverted lists
     * in the implementation of bulk_insert_entries().
     */
    typedef std::unordered_map<list_id_t, len_t> list_id_counts_map_t;

    /**
     * A type that represents an iterator over the slots
     * in the free_slots vector used to allocate and free slots.
     */
    typedef std::vector<StorageLists::Slot>::iterator slot_it_t;
    
    /**
     * In-memory data structure that maps list ids to inverted list objects.
     */
    list_id_list_map_t id_to_list_map;
    /**
     * Specifies the size of the mappping region in bytes.
     */
    size_t total_size;
    /**
     * Holds a pointer to the base of the memory-mapped region.
     */
    uint8_t *base_ptr;
    /**
     * Creates a new storage lists object.
     *
     * @param vector_dim The dimension of the vectors.
     * @param filename The name of the file to be used for storage.
     */
    StorageLists(const len_t vector_dim, const std::string &filename);

    StorageLists(){};

    /**
     * Destroys the storage lists object.
     *
     * Unmaps the memory-mapped region.
     */
    ~StorageLists();

    /**
     * Returns the number of inverted lists that are currently stored.
     *
     * @return The number of lists.
     */
    len_t get_length() const;

    /**
     * Returns the total size of the memory-mapped region.
     *
     * @return The total size of the region in bytes.
     */
    size_t get_total_size() const;

    /**
     * Returns the total number of bytes that are currently free across
     * all slots.
     *
     * @return The number of free bytes.
     */
    size_t get_free_space() const;

    /**
     * Returns the size of the largest continuous free space.
     *
     * @return The size of the largest continuous free space in bytes.
     */
    size_t get_largest_continuous_free_space() const;

    /**
     * Returns the total size of the vectors that can be stored.
     *
     * @return The size of a single vector in bytes.
     */
    size_t get_vector_size() const;

    /**
     * Returns the dimension of the vectors that can be stored.
     *
     * @return The dimension of the vectors.
     */
    len_t get_vector_dim() const;

    /**
     * Returns the name of the file used to store the inverted lists.
     *
     * @return The name of the file.
     */
    std::string get_filename() const;

    /**
     * Returns a pointer to the vectors of the given list.
     *
     * @param list_id The id of the list.
     * @return A pointer to the first vector of the list.
     * @throws std::invalid_argument If the list does not exist.
     */
    const vector_el_t *get_vectors(const list_id_t list_id) const;

    /**
     * Returns a pointer to the ids of the given list.
     *
     * @param list_id The id of the list.
     * @return A pointer to the first id of the list.
     * @throws std::invalid_argument If the list does not exist.
     */
    const vector_id_t *get_ids(const list_id_t list_id) const;

    /**
     * Returns the number of entries that are in used in the given list.
     *
     * @param list_id The id of the list.
     * @return The number of used entries.
     * @throws std::invalid_argument If the list does not exist.
     */
    len_t get_list_length(const list_id_t list_id) const;

    /**
     * Resizes the given list to the given number of entries.
     *
     * @param list_id The id of the list.
     * @param n_entries The new length of the list.
     * @throws std::invalid_argument If the list does not exist.
     * @throws std::out_of_range If the target length is 0.
     */
    void resize_list(const list_id_t list_id, const len_t n_entries);

    /**
     * Inserts the given entries into the given list.
     *
     * @param list_id The id of the list.
     * @param vectors A pointer to the first vector to insert.
     * @param ids A pointer to the first id to insert.
     * @param n_entries The number of entries to insert.
     */
    void insert_entries(const list_id_t list_id, const vector_el_t *vectors, const vector_id_t *ids, const len_t n_entries);

    /**
     * Updates the given entries in the given list.
     *
     * @param list_id The id of the list.
     * @param vectors A pointer to the first vector to update.
     * @param ids A pointer to the first id to update.
     * @param n_entries The number of entries to update.
     * @param offset The number of entries to skip before starting to update.
     * @throws std::invalid_argument If the list does not exist.
     * @throws std::out_of_range If the entries to be updated are out of bounds.
     */
    void update_entries(const list_id_t list_id, const vector_el_t *vectors, const vector_id_t *ids, const len_t n_entries, const size_t offset) const;

    /**
     * Creates a new inverted list
     * and allocates space for the given number of entries.
     *
     * @param list_id The id of the list.
     * @param n_entries The number of entries to allocate.
     * @throws std::invalid_argument If the list already exists.
     * @throws std::out_of_range If the number of entries is 0.
     */
    void create_list(
        const list_id_t list_id,
        const len_t n_entries);

    /**
     * Inserts the given entries into inverted lists with the given ids
     * where vectors, vector ids and list ids are stored in separate files.
     *
     * The binary files are expected to be little-endian where values
     * are expected to be stored consecutively in the following format:
     * - vectors: vector_dim * sizeof(vector_el_t) bytes per vector,
     *            the vector dimension is to be specified in the constructor.
     * - vector ids: sizeof(vector_id_t) bytes per id
     * - list ids: sizeof(list_id_t) bytes per id
     *
     * @param vectors_filename The name of the file containing the vectors.
     * @param vector_ids_filename The name of the file containing
     *                            the vector ids.
     * @param list_ids_filename The name of the file containing
     *                         the list ids.
     * @param n_entries The number of entries to insert.
     */
    void bulk_insert_entries(const std::string &vectors_filename, const std::string &vector_ids_filename, const std::string &list_ids_filename, const len_t n_entries);
  
  
  private:
  
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive & ar, const unsigned int version){
      ar & filename;
      ar & vector_dim;
      ar & vector_size;
      ar & total_size;
      // ar & base_ptr;
      ar & id_to_list_map;
      ar & free_slots;
    }

    /**
     * Specifies the location of the memory-mapped file
     */
    std::string filename;

    /**
     * Specifies the dimension of the vectors stored in the inverted lists
     */
    size_t vector_dim;

    /**
     * Specifies the size of the vectors stored in the inverted lists
     * in bytes.
     */
    size_t vector_size;

    /**
     * In-memory data structure that holds a list of free slots
     * in the memory-mapped file.
     */
    std::vector<Slot> free_slots;

    /**
     * Memory-maps the file used to store the inverted lists
     * containing the vectors and vector ids on disk
     * with read-write permissions.
     *
     * @throws std::runtime_error if the file cannot be opened or mapped.
     */
    void mmap_region();

    /**
     * Returns a pointer to the first vector within the memory region
     * associated with the given inverted list.
     *
     * All vectors of a list are stored contiguously in memory.
     *
     * @param list A pointer to the inverted list
     *             for which the vectors are being requested.
     * @return A pointer to the first vector.
     */
    vector_el_t *get_vectors_by_list(const InvertedList *list) const;

    /**
     * Returns a pointer to the first vector id within the memory region
     * associated with the given inverted list.
     *
     * All vector ids of a list are stored contiguously in memory.
     *
     * @param list A pointer to the inverted list
     *             for which the vector ids are being requested.
     * @return A pointer to the first vector id.
     */
    vector_id_t *get_ids_by_list(const InvertedList *list) const;

    /**
     * Returns total size by the given amount of vector ids
     * when they are stored contiguously in memory.
     *
     * @param n_entries The number of vector ids.
     * @return The total size in bytes.
     */
    size_t get_ids_size(const len_t n_entries) const;

    /**
     * Returns total size by the given amount of vectors
     * when they are stored contiguously in memory.
     *
     * @param n_entries The number of vectors.
     * @return The total size in bytes.
     */
    size_t get_vectors_size(const len_t n_entries) const;

    /**
     * Returns total size by the given amount of list ids
     * when they are stored contiguously in memory.
     *
     * @param n_entries The number of list ids.
     * @return The total size in bytes.
     */
    size_t get_list_ids_size(const len_t n_entries) const;

    /**
     * Returns total allocated size of the given inverted list in bytes.
     *
     * @param list A pointer to the inverted list.
     * @return The total size in bytes.
     */
    size_t get_total_list_size(const InvertedList *list) const;

    /**
     * Returns a slot reprensenting the memory region associated
     * with the given inverted list.
     *
     * Used when the inverted list is to be freed.
     *
     * @param list A pointer to the inverted list.
     * @return A slot of the location and size of the inverted list.
     */
    Slot list_to_slot(const InvertedList *list);

    /**
     * Performs the necessary steps to grow the memory-mapped region
     * to the given size.
     *
     * Adjusts the size of the file on disk and modifies the
     * list of free slots accordingly.
     *
     * @param new_size The new size of the memory-mapped region.
     * @throws std::runtime_error if new_size < total_size
     *         or if there is an error creating, opening or resizing the file
     *         or during un- or remapping of the file.
     */
    void resize_region(const size_t new_size);

    /**
     * Allocates a new inverted list with the given number of entries.
     *
     * The number of entries being allocated is always a power of two
     * and at least as large as the given used number of entries.
     *
     * @param n_entries The number of entries to be allocated.
     * @return An inverted list object representing the allocated memory region.
     */
    InvertedList alloc_list(const len_t n_entries);

    /**
     * Allocates a new slot of the given size.
     *
     * The memory region is grown if necessary. If a slot larger than the given
     * size is found, the inverted list is placed at the beginning of the slot
     * and the remaining space is added to the list of free slots.
     *
     * @param size The size of the slot to be allocated.
     * @return The offset of the allocated slot relative to the base pointer.
     */
    size_t alloc_slot(const size_t size);

    /**
     * Creates a free slot at the location and with the size of the given slot.
     *
     * The slot is added to the list of free slots. If the slot is adjacent
     * to one or two other slots, the slots are merged.
     *
     * @param slot A pointer to the slot to be freed.
     */
    void free_slot(const Slot *slot);

    /**
     * Finds the next power of two that is greater than or equal
     * to the given number.
     *
     * @param n The number to be rounded up.
     * @return The next power of two that is greater than or equal to n.
     */
    size_t round_up_to_next_power_of_two(const size_t n) const;

    /**
     * Checks if the list of free slots has a slot that extends to the end
     * of the memory-mapped region.
     *
     * @return True if there is a free slot at the end of the region.
     */
    bool has_free_slot_at_end() const;

    /**
     * If a file is already memory-mapped, it unmaps it.
     * If not, it creates a new file without memory-mapping it
     * in case the file does not already exist.
     */
    void ensure_file_created_and_region_unmapped() const;

    /**
     * Resizes the file to the given size.
     *
     * If the size is smaller than
     * the current size, the file is truncated.
     * If the size is larger than the current size,
     * the file is extended and padded with zeros.
     *
     * @param size The new size of the file.
     */
    void resize_file(const size_t size);

    /**
     * Checks if the given inverted list needs to be reallocated.
     *
     * Reallocation is necessary unless the list uses between 50% and 100%
     * of the allocated entries.
     *
     * @param list A pointer to the inverted list.
     * @param new_length Number of entries the list should be able to hold.
     * @return True if the list needs to be reallocated.
     */
    bool does_list_need_reallocation(const InvertedList *list, const len_t new_length) const;

    /**
     * Copy the entries of one inverted list to another.
     *
     * Only as many entries as the smaller list holds are copied.
     *
     * @param dst A pointer to the destination inverted list.
     * @param src A pointer to the source inverted list.
     */
    void copy_shared_data(const InvertedList *dst, const InvertedList *src) const;

    /**
     * Iterates over the list of free slots and finds the first slot
     * that is large enough to hold data of the given size.
     *
     * @param size The minimum size of the slot to be found in bytes.
     * @return An iterator to the slot.
     */
    slot_it_t find_large_enough_slot_index(const size_t size);

    /**
     * Grows the memory-mapped region until it is large enough to hold
     * at least the given number of bytes.
     *
     * The size of the region is always a power of two and has at least
     * a minimum size of MIN_TOTAL_SIZE_BYTES bytes.
     *
     * @param size The minimum size of the region to in bytes.
     */
    void grow_region_until_enough_space(size_t size);

    /**
     * Finds the next slot to the right of the given slot.
     *
     * If the given slot is the last slot, the end iterator is returned.
     *
     * @param slot A pointer to the slot.
     * @return An iterator to the next slot to the right.
     */
    slot_it_t find_next_slot_to_right(const Slot *slot);

    /**
     * Finds the next slot to the left of the given slot.
     *
     * If the given slot is the first slot, the end iterator is returned.
     *
     * @param next_slot_right An iterator to the slot to the right
     *                        of the slot to be found.
     * @return An iterator to the next slot to the left.
     */
    slot_it_t find_next_slot_to_left(const slot_it_t next_slot_right);

    /**
     * Checks if the given slots are adjacent.
     *
     * Two slots are adjacent if the right slot starts
     * at the end of the left slot.
     *
     * @param slot_left A pointer to the left slot.
     * @param slot_right A pointer to the right slot.
     * @return True if the slots are adjacent.
     */
    bool are_slots_adjacent(const Slot *slot_left, const Slot *slot_right) const;

    /**
     * Converts an iterator to a slot to a pointer to the slot.
     *
     * If the iterator is the end iterator, a null pointer is returned.
     *
     * @param it An iterator to a slot.
     * @return A pointer to the slot.
     */
    Slot *to_slot(const slot_it_t it) const;

    /**
     * Grows the memory-mapped region until it is large enough to hold
     * at least the given number of entries.
     *
     * @param n_entries The minimum number of entries
     *                  the region should be able to hold.
     */
    void reserve_space(const len_t n_entries);

    /**
     * Preallocates inverted lists given a file of list ids.
     *
     * @param entries_left A pointer to the map that should be populated
     *                     with the number of entries of each list.
     * @param list_ids_filename The name of the file containing the list ids.
     * @param n_entries The number of entries in the file.
     */
    void bulk_create_lists(list_id_counts_map_t &entries_left, const std::string &list_ids_filename, const len_t n_entries);

    /**
     * Creates a file stream for the given file.
     *
     * @param filename The name of the file.
     * @return A file stream for the file.
     */
    std::ifstream open_filestream(const std::string &filename) const;
  };
}