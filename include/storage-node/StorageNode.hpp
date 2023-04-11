#pragma once

#include "StorageIndex.hpp"

namespace ann_dkvs
{
  class StorageNode
  {
  private:
    const StorageIndex &index;

  public:
    StorageNode(const StorageIndex &index);
  };
}