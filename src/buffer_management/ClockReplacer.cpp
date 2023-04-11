#include <iostream>

#include "buffer_management/ClockReplacer.hpp"

namespace ann_dkvs {
ClockReplacer::ClockReplacer(int num_pages) {
    clock_pointer_ = 0;
    num_pages_ = num_pages;
    num_pinned_pages_ = 0;

    for (int i = 0; i < num_pages; i++) {
        used_frame_.push_back(false);
        pinned_.push_back(false);
        ref_flag_.push_back(false);
        /** Defaultly it is the first page, so it can be replaced. */
        first_frame_.push_back(true); 
    }
}

bool ClockReplacer::AccessFrame(frame_id_t frame_id, bool flag) {
    // std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id < 0 || frame_id >= num_pages_) {
        return false;
    }
    /**
     * point out that the frame is in used
     * set ref flag = true
    */
    used_frame_[frame_id] = true;
    ref_flag_[frame_id] = flag;

    return true;
}


void ClockReplacer::SetFirstFrame(frame_id_t frame_id, bool flag) {
    first_frame_[frame_id] = flag;
}

bool ClockReplacer::GetUsedFrame(frame_id_t frame_id) {
    return used_frame_[frame_id];
}

bool ClockReplacer::GetRefFlag(frame_id_t frame_id) {
    return ref_flag_[frame_id];
}


bool ClockReplacer::EvictNonFirstFrame() {
    // std::scoped_lock<std::mutex> lock(latch_);
    /** In principle, it won't access because non-first frames shouldn't be pinned as the first frame. */
    if (pinned_[clock_pointer_] || first_frame_[clock_pointer_] 
        || !used_frame_[clock_pointer_] 
        // || ref_flag_[clock_pointer_]
        ) {
        std::cout << "pinned: " << pinned_[clock_pointer_] << std::endl;
        std::cout << "first: " << first_frame_[clock_pointer_] << std::endl;
        std::cout << "used: " << used_frame_[clock_pointer_] << std::endl;
        std::cout << "flag: " << ref_flag_[clock_pointer_] << std::endl;
        std::cout << "WARNING: Error when evicting non-first frame!" << std::endl;
        pinned_[clock_pointer_] = false;

        return false;
    }
    
    SetFirstFrame(clock_pointer_, true);
    used_frame_[clock_pointer_] = false;
    ref_flag_[clock_pointer_] = false;

    clock_pointer_ = (clock_pointer_ + 1) % num_pages_;
    return true;
}

bool ClockReplacer::EvictFrame(frame_id_t *frame_id) {
    // std::scoped_lock<std::mutex> lock(latch_);
    if (num_pinned_pages_ == num_pages_) {
        std::cout << "All pages are pinned" << std::endl;
        return false;
    }
    
    /**
     * Go through at most num_pages_ frames each time.
    */
    for (int i = 0; i < num_pages_; i++) {
        /**
         * If the current frame isn't in used, return directly.
         * In principle, it won't be accessed.
        */
        // if (!used_frame_[clock_pointer_]) {
        //     std::cout << "WARNING: Find unused frame in VictimizeFrame()!" << std::endl;
        //     *frame_id = clock_pointer_;

        //     clock_pointer_ = (clock_pointer_ + 1) % num_pages_;
        //     return true;
        // }
        if (first_frame_[clock_pointer_]) {
            // std::cout << "used: " << used_frame_[clock_pointer_] << std::endl;
            // std::cout << "pinned: " << pinned_[clock_pointer_] << std::endl;
        }
        if (first_frame_[clock_pointer_] && used_frame_[clock_pointer_] && !pinned_[clock_pointer_]) {
            if (ref_flag_[clock_pointer_]) {
                ref_flag_[clock_pointer_] = false;
            } else {
                *frame_id = clock_pointer_;
                used_frame_[clock_pointer_] = false;
                // std::cout << "MSG: Evicting page " << clock_pointer_ << std::endl;

                clock_pointer_ = (clock_pointer_ + 1) % num_pages_;
                return true;
            }
        }
        clock_pointer_ = (clock_pointer_ + 1) % num_pages_;
        // std::cout << "Didn't find page to evict in one cycle!" << std::endl;
    }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    // std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id < 0 || frame_id >= num_pages_ || !used_frame_[frame_id]) {
        assert("Pin an unused frame!");
        return; 
    }

    if (!pinned_[frame_id]) {
        num_pinned_pages_++;
        pinned_[frame_id] = true;
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    // std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id < 0 || frame_id >= num_pages_ || !used_frame_[frame_id]) {
        assert("Unpin an unused frame!");
        return;
    }

    if (pinned_[frame_id]) {
        num_pinned_pages_--;
        pinned_[frame_id] = false;
    }
    
}
}