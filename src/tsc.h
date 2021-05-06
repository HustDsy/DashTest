
/**

 * tsc.h - support for using the TSC register on intel machines as a timing

 * method. Should compile with -O to ensure inline attribute is honoured.

 *

 * author: David Terei <code@davidterei.com>

 * copyright: Copyright (c) 2016, David Terei

 * license: BSD

 */



#include <stdint.h>
#include <stdio.h>


#define TSC_OVERHEAD_N 100000



// bench_start returns a timestamp for use to measure the start of a benchmark

// run.

static inline uint64_t bench_start(void)

{

  unsigned  cycles_low, cycles_high;

  asm volatile( "CPUID\n\t" // serialize

                "RDTSC\n\t" // read clock

                "MOV %%edx, %0\n\t"

                "MOV %%eax, %1\n\t"

                : "=r" (cycles_high), "=r" (cycles_low)

                :: "%rax", "%rbx", "%rcx", "%rdx" );

  return ((uint64_t) cycles_high << 32) | cycles_low;

}



// bench_end returns a timestamp for use to measure the end of a benchmark run.

static inline uint64_t bench_end(void)

{

  unsigned  cycles_low, cycles_high;

  asm volatile( "RDTSCP\n\t" // read clock + serialize

                "MOV %%edx, %0\n\t"

                "MOV %%eax, %1\n\t"

                "CPUID\n\t" // serialze -- but outside clock region!

                : "=r" (cycles_high), "=r" (cycles_low)

                :: "%rax", "%rbx", "%rcx", "%rdx" );

  return ((uint64_t) cycles_high << 32) | cycles_low;

}



// measure_tsc_overhead returns the overhead from benchmarking, it should be

// subtracted from timings to improve accuracy.

static uint64_t measure_tsc_overhead(void)

{

  uint64_t t0, t1, overhead = ~0;

  int i;



  for (i = 0; i < TSC_OVERHEAD_N; i++) {

    t0 = bench_start();

    asm volatile("");

    t1 = bench_end();

    if (t1 - t0 < overhead)

      overhead = t1 - t0;

  }



  return overhead;

}

/*
# TSC Frequency
To convert from cycles to wall-clock time we need to know TSC frequency
Frequency scaling on modern Intel chips doesn't affect the TSC.

Sadly, there doesn't seem to be a good way to do this.

# Intel V3B: 17.14
That rate may be set by the maximum core-clock to bus-clock ratio of the
processor or may be set by the maximum resolved frequency at which the
processor is booted. The maximum resolved frequency may differ from the
processor base frequency, see Section 18.15.5 for more detail. On certain
processors, the TSC frequency may not be the same as the frequency in the brand
string.

# Linux Source
http://lxr.free-electrons.com/source/arch/x86/kernel/tsc.c?v=2.6.31#L399

Linux runs a calibration phase where it uses some hardware timers and checks
how many TSC cycles occur in 50ms.
*/
#define TSC_FREQ_MHZ 2593

static inline uint64_t cycles_to_ns(uint64_t cycles)
{
  // XXX: This is not safe! We don't have a good cross-platform way to
  // determine the TSC frequency for some strange reason.
  return cycles * 1000 / TSC_FREQ_MHZ;
}
//
//int main(){
//	uint64_t t0, t1, overhead = ~0;
//	t0 = bench_start();
//
//    asm volatile("");
//
//    t1 = bench_end();
//
//    if (t1 - t0 < overhead)
//
//    overhead = t1 - t0;
//    printf("%lu", overhead);
//  	int i;
//} 
