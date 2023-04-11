#include <sys/mman.h>
#include <string.h>
#include <string>
#include <iostream>
#include <unistd.h>
#include <fstream>

#include "storage-node/StorageLists.hpp"

namespace ann_dkvs
{
  void StorageLists::mmap_region()
  {
    FILE *f = fopen(filename.c_str(), "r+");
    if (f == nullptr)
    {
      throw std::runtime_error("Could not open file " + filename);
    }
    base_ptr = (uint8_t *)mmap(
        nullptr,
        total_size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fileno(f),
        0);
    if (base_ptr == MAP_FAILED)
    {
      throw std::runtime_error("Could not mmap file " + filename);
    }
    fclose(f);
  }

  vector_el_t *StorageLists::get_vectors_by_list(const InvertedList *list) const
  {
    return (vector_el_t *)(base_ptr + list->offset);
  }

  vector_id_t *StorageLists::get_ids_by_list(const InvertedList *list) const
  {
    vector_el_t *vector_ptr = get_vectors_by_list(list);
    vector_id_t *id_ptr = (vector_id_t *)((size_t)vector_ptr + vector_size * list->allocated_entries);
    return id_ptr;
  }

  size_t StorageLists::get_vectors_size(const len_t n_entries) const
  {
    return n_entries * vector_size;
  }

  size_t StorageLists::get_ids_size(const len_t n_entries) const
  {
    return n_entries * sizeof(vector_id_t);
  }

  size_t StorageLists::get_list_ids_size(const len_t n_entries) const
  {
    return n_entries * sizeof(list_id_t);
  }

  size_t StorageLists::get_total_list_size(const InvertedList *list) const
  {
    len_t n_entries = list->allocated_entries;
    return get_vectors_size(n_entries) + get_ids_size(n_entries);
  }

  size_t StorageLists::get_free_space() const
  {
    size_t free_space = 0;
    for (auto slot : free_slots)
    {
      free_space += slot.size;
    }
    return free_space;
  }

  size_t StorageLists::get_largest_continuous_free_space() const
  {
    size_t max_free_space = 0;
    for (auto slot : free_slots)
    {
      if (slot.size > max_free_space)
      {
        max_free_space = slot.size;
      }
    }
    return max_free_space;
  }

  StorageLists::StorageLists(const len_t vector_dim, const std::string &filename) : filename(filename), vector_dim(vector_dim), vector_size(vector_dim * sizeof(vector_el_t)), total_size(0)
  {
    if (vector_dim == 0)
    {
      throw std::out_of_range("Vector dimension must be greater than 0");
    }
  }

  StorageLists::~StorageLists()
  {
    if (base_ptr != nullptr)
    {
      munmap(base_ptr, total_size);
    }
  }

  len_t StorageLists::get_length() const
  {
    return id_to_list_map.size();
  }

  size_t StorageLists::get_total_size() const
  {
    return total_size;
  }

  size_t StorageLists::get_vector_size() const
  {
    return vector_size;
  }

  len_t StorageLists::get_vector_dim() const
  {
    return vector_dim;
  }

  std::string StorageLists::get_filename() const
  {
    return filename;
  }

  bool StorageLists::has_free_slot_at_end() const
  {
    if (free_slots.size() == 0)
      return false;
    Slot last_slot = free_slots.back();
    return last_slot.offset + last_slot.size == total_size;
  }

  void StorageLists::ensure_file_created_and_region_unmapped() const
  {
    if (total_size == 0)
    {
      FILE *f = fopen(filename.c_str(), "r+");
      if (f == nullptr)
      {
        f = fopen(filename.c_str(), "w+");
        if (f == nullptr)
        {
          throw std::runtime_error("Could not create file " + filename);
        }
      }
      fclose(f);
    }
    else
    {
      munmap(base_ptr, total_size);
    }
  }

  void StorageLists::resize_file(const size_t size)
  {
    FILE *f = fopen(filename.c_str(), "r+");
    if (f == nullptr)
    {
      throw std::runtime_error("Could not open file " + filename);
    }
    if (ftruncate(fileno(f), size) == -1)
    {
      throw std::runtime_error("Could not resize file " + filename);
    }
    fclose(f);
  }

  void StorageLists::resize_region(const size_t new_size)
  {
    if (new_size < total_size)
    {
      throw std::runtime_error("Cannot shrink region");
    }
    if (new_size == total_size)
    {
      return;
    }
    ensure_file_created_and_region_unmapped();
    size_t size_to_grow = new_size - total_size;
    if (has_free_slot_at_end())
    {
      free_slots.back().size += size_to_grow;
    }
    else
    {
      Slot new_slot;
      new_slot.offset = total_size;
      new_slot.size = size_to_grow;
      free_slots.push_back(new_slot);
    }
    total_size = new_size;
    resize_file(total_size);
    mmap_region();
  }

  bool StorageLists::does_list_need_reallocation(const InvertedList *list, const len_t n_entries) const
  {
    return n_entries <= list->allocated_entries / 2 || n_entries > list->allocated_entries;
  }

  void StorageLists::copy_shared_data(const InvertedList *dst, const InvertedList *src) const
  {
    len_t n_entries_to_copy = std::min(dst->used_entries, src->used_entries);
    if (n_entries_to_copy == 0)
    {
      return;
    }
    memcpy(get_vectors_by_list(dst), get_vectors_by_list(src), get_vectors_size(n_entries_to_copy));
    memcpy(get_ids_by_list(dst), get_ids_by_list(src), get_ids_size(n_entries_to_copy));
  }

  StorageLists::Slot StorageLists::list_to_slot(const InvertedList *list)
  {
    Slot slot;
    slot.offset = list->offset;
    slot.size = get_total_list_size(list);
    return slot;
  }

  void StorageLists::resize_list(const list_id_t list_id, const len_t n_entries)
  {
    list_id_list_map_t::iterator list_it = id_to_list_map.find(list_id);
    if (list_it == id_to_list_map.end())
    {
      throw std::invalid_argument("List not found");
    }
    if (n_entries == 0)
    {
      throw std::out_of_range("Cannot resize list to 0 entries");
    }
    InvertedList *list = &list_it->second;
    if (!does_list_need_reallocation(list, n_entries))
    {
      list->used_entries = n_entries;
      return;
    }
    Slot slot = list_to_slot(list);
    free_slot(&slot);
    InvertedList new_list;
    new_list = alloc_list(n_entries);
    if (new_list.offset == list->offset)
    {
      memmove(get_ids_by_list(&new_list), get_ids_by_list(list), get_ids_size(list->used_entries));
    }
    else
    {
      copy_shared_data(&new_list, list);
    }
    id_to_list_map[list_id] = new_list;
  }

  StorageLists::InvertedList StorageLists::alloc_list(const len_t n_entries)
  {
    InvertedList list;
    list.used_entries = n_entries;
    const len_t min_list_length = (len_t)(MIN_N_ENTRIES_PER_LIST);
    len_t allocated_entries = std::max(n_entries, min_list_length);
    list.allocated_entries = round_up_to_next_power_of_two(allocated_entries);
    size_t list_size = get_total_list_size(&list);
    list.offset = alloc_slot(list_size);
    return list;
  }

  StorageLists::slot_it_t StorageLists::find_large_enough_slot_index(const size_t size)
  {
    for (auto it = free_slots.begin(); it != free_slots.end(); it++)
    {
      if (it->size >= size)
      {
        return it;
      }
    }
    return free_slots.end();
  }

  void StorageLists::grow_region_until_enough_space(size_t size)
  {
    size_t new_size = total_size == 0 ? MIN_TOTAL_SIZE_BYTES : total_size;
    if (has_free_slot_at_end())
    {
      size -= free_slots.back().size;
    }
    while (new_size - total_size < size)
    {
      new_size *= 2;
    }
    resize_region(new_size);
  }

  size_t StorageLists::alloc_slot(const size_t size)
  {
    auto slot_it = find_large_enough_slot_index(size);
    if (slot_it == free_slots.end())
    {
      grow_region_until_enough_space(size);
      slot_it = find_large_enough_slot_index(size);
    }
    Slot *slot = &(*slot_it);
    size_t offset = slot->offset;
    if (slot->size == size)
    {
      free_slots.erase(slot_it);
    }
    else
    {
      slot->offset += size;
      slot->size -= size;
    }
    return offset;
  }

  StorageLists::slot_it_t StorageLists::find_next_slot_to_right(const Slot *slot)
  {
    for (auto it = free_slots.begin(); it != free_slots.end(); it++)
    {
      if (it->offset > slot->offset)
      {
        return it;
      }
    }
    return free_slots.end();
  }

  StorageLists::slot_it_t StorageLists::find_next_slot_to_left(
      const slot_it_t next_slot_right)
  {
    if (next_slot_right == free_slots.begin())
    {
      return free_slots.end();
    }
    return prev(next_slot_right);
  }

  bool StorageLists::are_slots_adjacent(
      const Slot *slot_left, const Slot *slot_right) const
  {
    if (slot_left == nullptr || slot_right == nullptr)
    {
      return false;
    }
    return slot_left->offset + slot_left->size == slot_right->offset;
  }

  StorageLists::Slot *StorageLists::to_slot(const slot_it_t it) const
  {
    if (it == free_slots.end())
    {
      return nullptr;
    }
    return &(*it);
  }

  void StorageLists::free_slot(const Slot *slot)
  {
    slot_it_t slot_right_it = find_next_slot_to_right(slot);
    slot_it_t slot_left_it = find_next_slot_to_left(slot_right_it);
    Slot *slot_right = to_slot(slot_right_it);
    Slot *slot_left = to_slot(slot_left_it);
    bool has_adjacent_slot_left = are_slots_adjacent(slot_left, slot);
    bool has_adjacent_slot_right = are_slots_adjacent(slot, slot_right);
    if (has_adjacent_slot_left && has_adjacent_slot_right)
    {
      slot_left->size += slot->size + slot_right->size;
      free_slots.erase(slot_right_it);
    }
    else if (has_adjacent_slot_left && !has_adjacent_slot_right)
    {
      slot_left->size += slot->size;
    }
    else if (!has_adjacent_slot_left && has_adjacent_slot_right)
    {
      slot_right->offset -= slot->size;
      slot_right->size += slot->size;
    }
    else
    {
      free_slots.insert(slot_right_it, *slot);
    }
  }

  const vector_el_t *StorageLists::get_vectors(const list_id_t list_id) const
  {
    list_id_list_map_t::const_iterator list_it = id_to_list_map.find(list_id);
    if (list_it == id_to_list_map.end())
    {
      throw std::invalid_argument("List not found");
    }
    return get_vectors_by_list(&list_it->second);
  }

  const vector_id_t *StorageLists::get_ids(const list_id_t list_id) const
  {
    list_id_list_map_t::const_iterator list_it = id_to_list_map.find(list_id);
    if (list_it == id_to_list_map.end())
    {
      throw std::invalid_argument("List not found");
    }
    return get_ids_by_list(&list_it->second);
  }

  len_t StorageLists::get_list_length(const list_id_t list_id) const
  {
    list_id_list_map_t::const_iterator list_it = id_to_list_map.find(list_id);
    if (list_it == id_to_list_map.end())
    {
      throw std::invalid_argument("List not found");
    }
    return list_it->second.used_entries;
  }

  size_t StorageLists::round_up_to_next_power_of_two(const size_t n) const
  {
    size_t power = n;
    power--;
    power |= power >> 1;
    power |= power >> 2;
    power |= power >> 4;
    power |= power >> 8;
    power |= power >> 16;
    power |= power >> 32;
    power++;
    return power;
  }

  void StorageLists::create_list(
      const list_id_t list_id,
      const len_t n_entries)
  {
    if (id_to_list_map.find(list_id) != id_to_list_map.end())
    {
      throw std::invalid_argument("List already exists");
    }
    if (n_entries == 0)
    {
      throw std::out_of_range("List must have at least one entry");
    }
    InvertedList list = alloc_list(n_entries);
    id_to_list_map[list_id] = list;
  }

  void StorageLists::update_entries(
      const list_id_t list_id,
      const vector_el_t *vectors,
      const vector_id_t *ids,
      const len_t n_entries,
      const size_t offset) const
  {
    list_id_list_map_t::const_iterator list_it = id_to_list_map.find(list_id);
    if (list_it == id_to_list_map.end())
    {
      throw std::invalid_argument("List not found");
    }
    const InvertedList *list = &list_it->second;
    if (offset + n_entries > list->used_entries)
    {
      throw std::out_of_range("updating more entries than list has");
    }
    vector_el_t *list_vectors = get_vectors_by_list(list);
    vector_id_t *list_ids = get_ids_by_list(list);
    memcpy(list_vectors + offset * vector_dim, vectors, get_vectors_size(n_entries));
    memcpy(list_ids + offset, ids, get_ids_size(n_entries));
  }

  void StorageLists::insert_entries(
      const list_id_t list_id,
      const vector_el_t *vectors,
      const vector_id_t *ids,
      const len_t n_entries)
  {
    list_id_list_map_t::iterator list_it = id_to_list_map.find(list_id);
    InvertedList *list;
    if (list_it == id_to_list_map.end())
    {
      create_list(list_id, n_entries);
      list = &id_to_list_map[list_id];
    }
    else
    {
      list = &list_it->second;
    }
    resize_list(list_id, list->used_entries + n_entries);
    update_entries(list_id, vectors, ids, n_entries, list->used_entries - n_entries);
  }

  void StorageLists::reserve_space(const len_t n_entries)
  {
    if (n_entries == 0)
    {
      throw std::runtime_error("Cannot reserve 0 entries");
    }
    size_t size_to_reserve = get_vectors_size(n_entries) + get_ids_size(n_entries);
    grow_region_until_enough_space(size_to_reserve);
  }

  std::ifstream StorageLists::open_filestream(const std::string &filename) const
  {
    std::ifstream filestream(filename, std::ios::in | std::ios::binary);
    if (!filestream.is_open())
    {
      throw std::runtime_error("Could not open file " + filename);
    }
    return filestream;
  }

  void StorageLists::bulk_create_lists(
      list_id_counts_map_t &lists_counts,
      const std::string &list_ids_filename,
      const len_t n_entries)
  {
    std::ifstream list_ids_file = open_filestream(list_ids_filename);
    list_id_t list_id;
    len_t n_entries_read = 0;
    while (list_ids_file.read((char *)&list_id, sizeof(list_id_t)))
    {
      lists_counts[list_id]++;
      n_entries_read++;
    }
    list_ids_file.close();
    if (list_ids_file.bad())
    {
      throw std::runtime_error("Error reading list ids file");
    }
    if (n_entries_read != n_entries)
    {
      throw std::runtime_error("Number of entries in list ids file does not match n_entries");
    }

    for (auto list_it : lists_counts)
    {
      create_list(list_it.first, list_it.second);
    }
  }

  void StorageLists::bulk_insert_entries(
      const std::string &vectors_filename,
      const std::string &ids_filename,
      const std::string &list_ids_filename,
      const len_t n_entries)
  {
    if (total_size != 0)
    {
      throw std::runtime_error("bulk_insert_entries() can only be called on an empty inverted lists object");
    }

#if DYNAMIC_INSERTION == 0
    std::cout << "Bulk insertion." << std::endl;
    reserve_space(n_entries);
    list_id_counts_map_t entries_left;
    bulk_create_lists(entries_left, list_ids_filename, n_entries);
#endif

    std::ifstream vectors_file = open_filestream(vectors_filename);
    std::ifstream ids_file = open_filestream(ids_filename);
    std::ifstream list_ids_file = open_filestream(list_ids_filename);

    const len_t max_buffer_size = (len_t)(MAX_BUFFER_SIZE);
    const len_t buffer_size = std::min(n_entries, max_buffer_size);

    vector_el_t *vectors = (vector_el_t *)malloc(get_vectors_size(buffer_size));
    vector_id_t *vector_ids = (vector_id_t *)malloc(get_ids_size(buffer_size));
    list_id_t *list_ids = (list_id_t *)malloc(get_list_ids_size(buffer_size));

    len_t n_entries_read = 0;

    while (n_entries_read < n_entries)
    {
      std::cout << "n_entries_read: " << n_entries_read << std::endl;
      
      len_t n_entries_to_read = std::min(buffer_size, n_entries - n_entries_read);

      if (!vectors_file.read((char *)vectors, get_vectors_size(n_entries_to_read)))
      {
        throw std::runtime_error("Error reading vectors file");
      }
      if (!ids_file.read((char *)vector_ids, get_ids_size(n_entries_to_read)))
      {
        throw std::runtime_error("Error reading ids file");
      }
      if (!list_ids_file.read((char *)list_ids, get_list_ids_size(n_entries_to_read)))
      {
        throw std::runtime_error("Error reading list ids file");
      }

      for (len_t i = 0; i < n_entries_to_read; i++)
      {
        vector_el_t *vector = &vectors[i * vector_dim];
        vector_id_t *vector_id = &vector_ids[i];
        list_id_t list_id = list_ids[i];

#if DYNAMIC_INSERTION == 1
        insert_entries(list_id, vector, vector_id, 1);
#else
        len_t list_length = get_list_length(list_id);
        len_t cur_list_offset = list_length - entries_left[list_id];
        update_entries(list_id, vector, vector_id, 1, cur_list_offset);
        entries_left[list_id]--;
#endif
      }

      n_entries_read += n_entries_to_read;
    }

    free(vectors);
    free(vector_ids);
    free(list_ids);

    if (vectors_file.fail() || ids_file.fail() || list_ids_file.fail())
    {
      throw std::runtime_error("Error reading files");
    }

    vectors_file.close();
    ids_file.close();
    list_ids_file.close();
  }
}