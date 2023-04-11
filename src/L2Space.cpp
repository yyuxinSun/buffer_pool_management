#include "L2Space.hpp"

namespace ann_dkvs
{

  L2Space::L2Space(size_t vector_dim) : vector_dim(vector_dim)
  {
    distance_func = L2Sqr;

#ifdef __AVX__
    if (vector_dim % 16 == 0)
    {
      distance_func = L2SqrSIMD16ExtAVX;
    }
    else if (vector_dim > 16)
    {
      distance_func = L2SqrSIMD16ExtResiduals;
    }
#endif
  }

  distance_func_t L2Space::get_distance_func() const
  {
    return distance_func;
  }

  len_t L2Space::get_vector_dim() const
  {
    return vector_dim;
  }
}
