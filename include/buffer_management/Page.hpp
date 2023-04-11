#pragma once
#include <cstring>

#include "../storage-node/types.hpp"

namespace ann_dkvs {
/**
 * Page class represents the page / frame in the buffer pool.
*/
class Page{
    friend class BufferPoolManager;
    public:
        Page() { ResetMemory(); }

        ~Page() = default;

        inline auto GetVectors() -> vector_el_t* { return vectors_; }
        inline auto GetIDs() -> vector_id_t* { return ids_; }
        inline auto GetPinCount() -> int { return pin_count_; }
        inline auto GetListID() -> list_id_t { return list_id_; }
        inline auto GetAccessTimes() -> int { return access_times_; }
        inline auto GetListSize() -> int { return list_size_; }

    // private:
        inline void ResetMemory() { 
            memset(vectors_, -1, sizeof(vectors_));
            memset(ids_, -1, sizeof(ids_));
        }
        // char vectors_[FRAME_DATA_SIZE * sizeof(vector_el_t)]{};
        // char ids_[FRAME_DATA_SIZE * sizeof(vector_id_t)]{};
        vector_el_t vectors_[FRAME_DATA_SIZE]{};
        vector_id_t ids_[FRAME_DATA_NUM]{};

        list_id_t list_id_ = INVALID_LIST_ID;
        int pin_count_ = 0;
        // int num_pages_occupied_ = -1; //  if linked, set it to the next linked page's frame_id.
        // bool first_page_ = true; // Whether it is the first page of a list. 
                                    // Defaultly true, because defaultly all can be evicted.

        /**
         * Compare the access_times with list_len_.
         * Decide the parameter flag of ClockReplacer::AccessFrame()
        */
        int access_times_ = 0;
        int list_size_ = 0; /** How many pages in buffer the list occupied. */
};

}