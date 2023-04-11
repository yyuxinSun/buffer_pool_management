#include "buffer_management/BufferPoolManager.hpp"
#include <cassert>
#include <math.h>
#include <iostream>

namespace ann_dkvs {
BufferPoolManager::BufferPoolManager(size_t pool_size, const StorageLists* lists, std::string filename)
    : pool_size_(pool_size) {
    
    /** Binary mode to read. */
    // db_io_.open(filename, std::ios::binary | std::ios::in);
    // assert(db_io_.is_open() || !"Cannot open the lists file on disk!");
    db_io_ = open(filename.c_str(), O_RDONLY);
    int flags = fcntl(db_io_, F_GETFL, 0);
    fcntl(db_io_, F_SETFL, flags | O_NONBLOCK);
    assert(db_io_ != -1 || !"Cannot open the lists file on disk!");

    pages_ = new Page[pool_size_];
    replacer_ = new ClockReplacer(pool_size_);
    free_num_ = pool_size_;
    
    /** All pages / frames are free in the beginning. */
    for (size_t i = 0; i < pool_size_; i++) {
        free_list_.push_back(true);
    }

    size_t vector_size = lists->get_vector_size(); 
    for (size_t i = 0; i < NUM_LISTS; i++) {
        /** Initialize the hash table: list_id => list_size. */
        size_t list_size = lists->get_list_length(i);
        // size_t list_pages_size = ceil( list_size / (double) 56.0 );
        hash_to_list_size_.insert(std::make_pair((list_id_t) i, list_size));

        /** Initialize the hash table: list_id => (vectors_offset, ids_offset). */
        StorageLists::list_id_list_map_t::const_iterator list_it = lists->id_to_list_map.find(i);
        assert(list_it != lists->id_to_list_map.end() || !"Cannot find the list id from the disk file!");

        const StorageLists::InvertedList *list = &list_it->second;
        size_t vectors_offset = list->offset;
        size_t ids_offset = list->offset + vector_size * list->allocated_entries;

        hash_to_disk_vectors_.insert(std::make_pair( (list_id_t) i, std::make_pair(vectors_offset, ids_offset)) );
        hash_to_buffer_pages_.insert(std::make_pair((list_id_t) i, -1));
    }
}

int BufferPoolManager::ListPageSize(list_id_t list_id) {
    int vectors_num = hash_to_list_size_[list_id];
    return ceil( vectors_num / (double) FRAME_DATA_NUM);
}

int BufferPoolManager::LookUpFreeList(int size) {
    /** 0 / 1, whether it is the first free frame (value == true). */
    if (free_num_ < size) {
        return -1;
    }
    int flag = 0; 
    int continuous_free_size = 0;
    int start_frame = -1;
    for (int i = 0; i < pool_size_; i++) {
        if (free_list_[i] == false) {
            flag = 0;
            continuous_free_size = 0;
        } else {
            continuous_free_size++;
            if (flag == 0) {
                start_frame = i;
                flag = 1;
            }
            if (continuous_free_size >= size) {
                return start_frame;
            }
        }
    }

    return -1;
}

void BufferPoolManager::AllocateFreeFrames(std::vector<frame_id_t>& found_pages, int evict_frame, int fetch_size) {
    for (int i = evict_frame; i < fetch_size + evict_frame; i++) {
        assert(free_list_[i] == true || !"Error in LookUpFreeList()! Try to evict a used frame as free!");
        found_pages.push_back((frame_id_t) i);
        free_list_[i] = false;
        free_num_--;
    }
}

void BufferPoolManager::ResetFrame(frame_id_t frame_id) {
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].access_times_ = 0;
    pages_[frame_id].list_size_ = 0;
    pages_[frame_id].list_id_ = INVALID_LIST_ID;

    pages_[frame_id].ResetMemory();
}

void BufferPoolManager::AccessList(frame_id_t frame_id, int list_size) {
    for (int i = 0; i < list_size; i++) {
        assert(frame_id + i < pool_size_ || !"1: In principle a list cannot be cycled!");

        pages_[frame_id + i].pin_count_++;
        pages_[frame_id + i].access_times_++;

        if (!replacer_->GetUsedFrame(frame_id + i)) {
            replacer_->AccessFrame(frame_id + i, false);
        }
        assert(replacer_->GetUsedFrame(frame_id + i) || !"Failed to set unused!");

        replacer_->Pin(frame_id + i);
        if (pages_[frame_id + i].access_times_ == pages_[frame_id + i].list_size_) {
            replacer_->AccessFrame(frame_id + i, true);
            pages_[frame_id + i].access_times_ = 0;
        }
    }
}

void BufferPoolManager::UpdateSingleFrame(frame_id_t frame_id, size_t vectors_offset, size_t ids_offset, size_t item_num) {
    /** Set the content of vectors_. */
    size_t vectors_size = item_num * sizeof(vector_el_t) * DATA_DIMENSION;
    // db_io_.seekg(vectors_offset);
    // db_io_.read((char*) pages_[frame_id].GetVectors(), vectors_size);
    // assert(!db_io_.bad() || !"I/O error when reading file for vectors_!");

    // int read_count = db_io_.gcount();
    // if (read_count != vectors_size) {
    //     db_io_.clear();
    //     pages_[frame_id].ResetMemory();
    //     assert("Vectors' reading bytes are inconsistent!");
    // }


    lseek(db_io_, vectors_offset, SEEK_SET);
    read(db_io_, (char*) pages_[frame_id].GetVectors(), vectors_size);
    // ssize_t read_vectors = pread(db_io_, (char*) pages_[frame_id].GetVectors(), vectors_size, vectors_offset);
    // assert(read_vectors != -1 || !"I/O error when reading file for vectors_!");
    
    /** Set the content of ids_. */
    size_t ids_size = item_num * sizeof(vector_id_t);
    // db_io_.seekg(ids_offset);
    // db_io_.read((char*) pages_[frame_id].GetIDs(), ids_size);
    // assert(!db_io_.bad() || !"I/O error when reading file for ids_!");

    // read_count = db_io_.gcount();
    // if (read_count != ids_size) {
    //     db_io_.clear();
    //     pages_[frame_id].ResetMemory();
    //     assert("IDs' reading bytes are inconsistent!");
    // }


    lseek(db_io_, ids_offset, SEEK_SET);
    read(db_io_, (char*) pages_[frame_id].GetIDs(), ids_size);
    // ssize_t read_ids = pread(db_io_, (char*) pages_[frame_id].GetIDs(), ids_size, ids_offset);
    // assert(read_ids != -1 || !"I/O error when reading file for vectors_!");
}

void BufferPoolManager::UpdateFrames(std::vector<frame_id_t> frame_ids, list_id_t list_id) {
    size_t list_size = frame_ids.size();
    size_t vectors_start_offset = hash_to_disk_vectors_[list_id].first;
    size_t ids_start_offset = hash_to_disk_vectors_[list_id].second;

    size_t vectors_bytes_per_page = FRAME_DATA_SIZE * sizeof(vector_el_t);
    size_t ids_bytes_per_page = FRAME_DATA_NUM * sizeof(vector_id_t);

    for (size_t i = 0; i < list_size; i++) {
        frame_id_t frame_id = frame_ids[i];
        assert(replacer_->GetFirstFrame(frame_id) == true || !"Logical error for first_frame_ value when checking UpdateFrames()!");

        pages_[frame_id].list_id_ = list_id;
        pages_[frame_id].list_size_ = list_size;
        if (i == 0) {
            replacer_->SetFirstFrame(frame_id, true);
        } else {
            replacer_->SetFirstFrame(frame_id, false);
        }

        size_t vectors_offset = vectors_start_offset + i * vectors_bytes_per_page;
        size_t ids_offset = ids_start_offset + i * ids_bytes_per_page;

        if (i != list_size - 1) {
            UpdateSingleFrame(frame_id, vectors_offset, ids_offset, FRAME_DATA_NUM);
        } else {
            size_t last_page_num = hash_to_list_size_[list_id] % FRAME_DATA_NUM;
            size_t last_page_size = last_page_num == 0 ? FRAME_DATA_NUM : last_page_num;
            UpdateSingleFrame(frame_id, vectors_offset, ids_offset, last_page_size);
        }
    }
}

std::vector<frame_id_t> BufferPoolManager::FetchListPages(list_id_t list_id) {
    // std::scoped_lock<std::mutex> lock(latch_);

    total++;
    
    frame_id_t frame_id;
    int fetch_size = ListPageSize(list_id);
    std::vector<frame_id_t> found_pages;

    /** Found the list in the buffer pool. */
    // auto iter = hash_to_buffer_pages_.find(list_id);

    frame_id_t found_id = hash_to_buffer_pages_[list_id];
    if (found_id != -1) {
    // if (iter != hash_to_buffer_pages_.end()) {

        hit++;

        // frame_id = iter->second;
        frame_id = found_id;
        AccessList(frame_id, fetch_size);
        for (int i = 0; i < fetch_size; i++) {
            assert(frame_id + i < pool_size_ || !"2: In principle a list cannot be cycled!");
            found_pages.push_back(frame_id + i);
        }

        return found_pages;
    }

    /** Didn't find the list in the buffer pool. */
    int evict_frame = -1;

    /** Look up the free_list first. */
    if (free_num_ >= fetch_size) {
        evict_frame = LookUpFreeList(fetch_size);
        /** Has enough continuous free space. No need to evict from buffer pool. */
        if (evict_frame != -1) {
            AllocateFreeFrames(found_pages, evict_frame, fetch_size);
            frame_id = evict_frame;
        }
    }
    /** Need to evict some pages / frames from the buffer pool. */
    while (evict_frame == -1) {
        bool evict_success = replacer_->EvictFrame(&frame_id);

        if (evict_success) {
            int evict_size = pages_[frame_id].list_size_;
            list_id_t evict_list_id = pages_[frame_id].list_id_;

            free_list_[frame_id] = true;
            free_num_++;
            ResetFrame(frame_id);

            /** Update the evicted frames in the free space. */
            for (int i = 1; i < evict_size; i++) {
                bool evict_non_first = replacer_->EvictNonFirstFrame();

                assert(evict_non_first || !"Logical error when evicting non-first frames!");
                assert(frame_id + i < pool_size_ || !"3: In principle a list cannot be cycled!");

                free_list_[i + frame_id] = true;
                free_num_++;
                ResetFrame(i + frame_id);
            }
        
            // hash_to_buffer_pages_.erase(evict_list_id);
            hash_to_buffer_pages_[evict_list_id] = -1;
            // std::cout << "Evict list is: " << evict_list_id << std::endl;

            evict_frame = LookUpFreeList(fetch_size);
            if (evict_frame != -1) {
                AllocateFreeFrames(found_pages, evict_frame, fetch_size);
            }
        }
    }

    assert(found_pages.size() == fetch_size || !"Error when setting found_pages (wrong number of result frames)!");
    assert(found_pages[0] == evict_frame || !"Logical error when allocating free frames!");

    // hash_to_buffer_pages_.insert(std::make_pair(list_id, found_pages[0]));
    hash_to_buffer_pages_[list_id] = found_pages[0];
    UpdateFrames(found_pages, list_id);
    AccessList(found_pages[0], fetch_size);
    return found_pages;
}

bool BufferPoolManager::UnPinListPages(list_id_t list_id) {
    // std::scoped_lock<std::mutex> lock(latch_);

    // auto iter = hash_to_buffer_pages_.find(list_id);
    // assert(iter != hash_to_buffer_pages_.end() || !"Try to unpin a list not in the buffer pool!");
    // frame_id_t frame_id = iter->second;

    frame_id_t frame_id = hash_to_buffer_pages_[list_id];
    assert(frame_id != -1 || !"Try to unpin a list not in the buffer pool!");

    assert(pages_[frame_id].pin_count_ != 0 || !"1: Unpin a non-pin list!");

    int list_size = ListPageSize(list_id);
    for (int i = 0; i < list_size; i++) {
        assert(frame_id + i < pool_size_ || !"4: In principle a list cannot be cycled!");
        assert(pages_[frame_id + i].pin_count_ != 0 || !"2: Unpin a non-pin list!");

        pages_[frame_id + i].pin_count_--;
        if (pages_[frame_id + i].pin_count_ == 0) {
            replacer_->Unpin(frame_id + i);
        }
    }
}

BufferPoolManager::~BufferPoolManager() {
    delete[] pages_;
    delete replacer_;
    // db_io_.close();
    if (close(db_io_) < 0) {
        assert("Failed to close the disk file!");
    }
}

}