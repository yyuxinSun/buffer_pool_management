#pragma once

#ifdef __AVX__
#include <immintrin.h>
#endif

#include "storage-node/types.hpp"

#define PORTABLE_ALIGN32 __attribute__((aligned(32)))

namespace ann_dkvs
{
  static inline float L2Sqr(
      const void *pVect1v,
      const void *pVect2v,
      const void *qty_ptr)
  {
    float *pVect1 = (float *)pVect1v;
    float *pVect2 = (float *)pVect2v;
    size_t qty = *((size_t *)qty_ptr);

    float res = 0;
    for (size_t i = 0; i < qty; i++)
    {
      float t = *pVect1 - *pVect2;
      pVect1++;
      pVect2++;
      res += t * t;
    }
    return (res);
  }

#ifdef __AVX__
  static inline float L2SqrSIMD16ExtAVX(const void *pVect1v, const void *pVect2v, const void *qty_ptr)
  {
    float *pVect1 = (float *)pVect1v;
    float *pVect2 = (float *)pVect2v;
    size_t qty = *((size_t *)qty_ptr);
    float PORTABLE_ALIGN32 TmpRes[8];
    size_t qty16 = qty >> 4;

    const float *pEnd1 = pVect1 + (qty16 << 4);

    __m256 diff, v1, v2;
    __m256 sum = _mm256_set1_ps(0);

    while (pVect1 < pEnd1)
    {
      v1 = _mm256_loadu_ps(pVect1);
      pVect1 += 8;
      v2 = _mm256_loadu_ps(pVect2);
      pVect2 += 8;
      diff = _mm256_sub_ps(v1, v2);
      sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));

      v1 = _mm256_loadu_ps(pVect1);
      pVect1 += 8;
      v2 = _mm256_loadu_ps(pVect2);
      pVect2 += 8;
      diff = _mm256_sub_ps(v1, v2);
      sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
    }

    _mm256_store_ps(TmpRes, sum);
    return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];
  }

  static inline float L2SqrSIMD16ExtResiduals(const void *pVect1v, const void *pVect2v, const void *qty_ptr)
  {
    size_t qty = *((size_t *)qty_ptr);
    size_t qty16 = qty >> 4 << 4;
    float res = L2SqrSIMD16ExtAVX(pVect1v, pVect2v, &qty16);
    float *pVect1 = (float *)pVect1v + qty16;
    float *pVect2 = (float *)pVect2v + qty16;

    size_t qty_left = qty - qty16;
    float res_tail = L2Sqr(pVect1, pVect2, &qty_left);
    return (res + res_tail);
  }

#endif

  using distance_func_t = distance_t (*)(const void *, const void *, const void *);
  class L2Space
  {
  private:
    size_t vector_dim;
    distance_func_t distance_func;

  public:
    L2Space(size_t vector_dim);
    distance_func_t get_distance_func() const;
    size_t get_vector_dim() const;
  };

} // namespace ann_dkvs