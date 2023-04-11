#pragma once
#include <vector>
#include <mutex>

#include "../storage-node/types.hpp"

namespace ann_dkvs {
class ClockReplacer {
 public:
   /**
   * Create a new ClockReplacer.
   * @param num_pages is the size of the ClockReplacer
   */
  ClockReplacer(int num_pages);

  virtual ~ClockReplacer() = default;

  /**
   * User accesses a frame, to either read from existing page at frame
   * or add a new page to that frame
   * @param int representing frame to be accessed
   * @return true if successfully accessed a frame
   */
  bool AccessFrame(frame_id_t frame_id, bool flag);

  /**
   * Least recently used frame according to clock algorithm is removed
   * @param pointer to int representing frame id removed
   * @return true if operation is successful
   */
  bool EvictFrame(frame_id_t *frame_id);

  /**
   * Different from the previous victim function, 
   * it victim the frame which is pointed by the clock pointer currently.
   * In principle, it is only called by the buffer pool manager to victim the non-first page of an evicted list.
  */
  bool EvictNonFirstFrame();

  /**
   * Call from the buffer pool manager to tell the replacer whether this page is used as the first page of a list.
  */
  void SetFirstFrame(frame_id_t frame_id, bool flag);
  // void SetUsedFrame(frame_id_t frame_id, bool flag);
  // void SetRefFlag(frame_id_t frame_id, bool flag);

  bool GetFirstFrame(frame_id_t frame_id) { return first_frame_[frame_id]; }


  /**
   * A frame is pinned, meaning it can't be victimized.
   * Accessing a pinned frame won't have any affect because the frame 
   * is not in the clock replacer.
   * @param int representing frame to be pinned
   */
  void Pin(frame_id_t frame_id);

  /**
   * Puts frame back into replacement consideration
   * @param int representing frame to be unpinned
   */
  void Unpin(frame_id_t frame_id);

  /**
   * @return number of frames currently in clock replacer that are unpinned
   *         (which can be victimized)
   */
  // int Size();

  bool GetUsedFrame(frame_id_t frame_id);

  bool GetRefFlag(frame_id_t frame_id);

 private:
  int clock_pointer_;
  int num_pages_;
  int num_pinned_pages_;
  std::mutex latch_;
  // int numFramesInClockReplacer;

  std::vector<bool> used_frame_; // True if frame being used
  std::vector<bool> ref_flag_; // Stores reference bits
  std::vector<bool> pinned_; // True if pinned
  std::vector<bool> first_frame_; // Whether it is the first page of a list.
};

}