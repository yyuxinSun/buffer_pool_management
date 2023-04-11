#pragma once

#include "RootIndex.hpp"

namespace ann_dkvs
{
  class RootNode
  {
  private:
    const RootIndex &index;

  public:
    RootNode(const RootIndex &index);
  };
}