#pragma once
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>
// #include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <string>

#include "ClockReplacer.hpp"
#include "Page.hpp"
#include "../storage-node/types.hpp"
#include "../storage-node/StorageLists.hpp"

namespace ann_dkvs {
class BufferPoolManager {
    public: 
        int total = 0;
        int hit = 0;
        
        BufferPoolManager(size_t pool_size, const StorageLists* list, std::string filename);
        ~BufferPoolManager();

        /**
         * Return the ids of frames / pages in the buffer pool, which store the content of the list.
        */
        auto FetchListPages(list_id_t list_id) -> std::vector<frame_id_t>;
        /**
         * Unpin the page / frame.
        */
        auto UnPinListPages(list_id_t list_id) -> bool;

        auto GetPageVectors(frame_id_t frame_id) -> vector_el_t* { return pages_[frame_id].GetVectors(); }

        auto GetPageIDs(frame_id_t frame_id) -> vector_id_t* { return pages_[frame_id].GetIDs(); }

    private:
        /** Number of pages in the buffer. */
        const size_t pool_size_;
        /** Array of pages in the buffer pool.  */
        Page* pages_;
        /** Hash from list id to the first frame id. */
        std::unordered_map<list_id_t, frame_id_t> hash_to_buffer_pages_;
        /** Hash from list id to disk address (of vectors and ids). */
        std::unordered_map<list_id_t, std::pair<size_t, size_t> > hash_to_disk_vectors_;
        /** Hash from list id to list length. (length represents how many vectors it contains) */
        std::unordered_map<list_id_t, int> hash_to_list_size_;
        /** Replacer to find unpinned pages to replace. */
        ClockReplacer* replacer_;
        /** Point out whether the frame is free or not. */
        std::vector<bool> free_list_;
        /** Base pointer of the file on disk. */
        // std::fstream db_io_;
        int db_io_;
        /** Latch */
        std::mutex latch_;
        /** Number of unused pages / frames in the buffer pool. */
        int free_num_;

        /** We need to reset the contents of a list frame in the buffer pool. */
        void UpdateFrames(std::vector<frame_id_t> frame_ids, list_id_t list_id);
        /** Update the data in a single frame. Offsets is calculated in UpdateFrames. 
         * Offset represents the offset to the beginning of the file.
         * item_num represents the number of item to copy.
         */
        void UpdateSingleFrame(frame_id_t frame_id, size_t vectors_offset, size_t ids_offset, size_t bytes_num);
        
        /** Lookup the free_list to find the **BEST** continuous free space which is large enough to hold the list. */
        int LookUpFreeList(int size);

        void AllocateFreeFrames(std::vector<frame_id_t>& found_pages, int evict_frame, int fetch_size);

        /** Reset the content of the frame / page to the initial state. */
        void ResetFrame(frame_id_t frame_id);

        /** The list is accessed by the query. Size represents the size of the list. */
        void AccessList(frame_id_t frame_id, int list_size);
        /** Return how many pages the list occupies in buffer pool. */
        int ListPageSize(list_id_t list_id);
};

}