//
// Created by dsy on 2021/4/16.
//

#include<iostream>
#include<fstream>
#include<string>
#include <gflags/gflags.h>
#include <immintrin.h>
#include <sys/time.h>
#include <unistd.h>
#include<cstdio>
#include <atomic>
#include<pthread.h>


#include "Hash.h"
#include "allocator.h"
#include "ex_finger.h"
#include "libpmemobj.h"


#define READ 0
#define INSERT 1
#define KEY_LEN 20
#define VALUE_LEN 100
#define HASH_SEED 0xc70697UL

const char *pool_name = "/data/pmem0/dash";
const size_t pool_size= 1024ul * 1024ul * 1024ul * 30ul;//30G
// Set Test Config
std::string workload = "workloade";
uint8_t thread_num = 72;
std::string mode = "run";
const char *default_path;


struct op_entry {
  int op;      // 0 read 1 insert
  char key[KEY_LEN];
  char value[VALUE_LEN];
};
/**
 * entries:任务数组
 * task:最多size大小的任务
 */
struct task{
  op_entry *entries;
  uint32_t size;
};


struct thread_input {
  task *t;
  Hash<string_key*>*index;//属于哪一个索引
  uint32_t idx;
};

/**
 * 读取操作文件，封装数据到 input 中
 * @param path
 * @param input
 */
// 统计origin key len小于20的情况，最终计算结果的时候需要减掉这部分数据量
long read_error_len = 0;
long insert_error_len = 0;
// 统计插入和读取的ycsb数据总量
long read_total_num = 0;
long insert_total_num = 0;

/**
 * 假设所有RAM,PMEM在Socket0之上，且物理架构如下所示
 *  125GB DRAM 512GB(?GiB) PEMM
 *  NUMA node0 CPU(s):               0-17,36-53
 *  NUMA node1 CPU(s):               18-35,54-71
 *  SocketID | MappedMemoryLimit | TotalMappedMemory
 * ==================================================
 * 0x0000   | 1024.000 GiB      | 632.000 GiB
 * 0x0001   | 1024.000 GiB      | 0.000 GiB
 *
 */
void set_affinity(uint32_t idx) {
    assert(idx <= 72);
    cpu_set_t my_set;
    CPU_ZERO(&my_set);
    int ret = 0;

    if (idx < 18 || idx >= 54) {
        CPU_SET(idx, &my_set);
        // printf("set affinity %u\n", idx);
    } else if (idx < 36) {
        CPU_SET(idx + 18, &my_set);
        // printf("set affinity %u\n", idx + 18);
    } else if (idx < 54) {
        CPU_SET(idx - 18, &my_set);
        // printf("set affinity %u\n", idx - 18);
    }
    ret = sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
    assert(ret == 0);
}


void read_input_files(const char *path, task *t, uint8_t t_size) // t_size是线程数
{


  char buffer[1000];
  FILE *fp = fopen(path, "r");
  if (!fp) return;

  //某个线程队列的位置）
  int *cursor = static_cast<int *>(calloc(t_size, sizeof(int)));

  while (!feof(fp)) {
    memset(buffer, 0, sizeof(buffer));
    fgets(buffer, sizeof(buffer), fp); // 包含了换行符
    if (!strcmp(buffer, "insert\n")) {
      // read key
      fgets(buffer, sizeof(buffer), fp);
      uint64_t hash_key = xxhash(buffer, KEY_LEN, HASH_SEED);

      const uint32_t idx = hash_key % t_size;

      // check 对应线程任务队列是否已满
      if (t[idx].size == cursor[idx]) {
        t[idx].entries = static_cast<op_entry *>(
            realloc(t[idx].entries, sizeof(struct op_entry) * t[idx].size * 2));
        t[idx].size *= 2;
      }
      // set op type
      t[idx].entries[cursor[idx]].op = INSERT;

      int str_len = strlen(buffer) - 1; // ycsb文件中每一行最后有换行符需要考虑,所以通过strlen函数读出来的长度要比真实显示的情况+1
      int offset = str_len - 20; // 取偏移量
      if(str_len < 20) {
        insert_error_len++;
        //printf("[insert %ld] ycsb origin key_length < 20 \n", insert_error_len);
        continue;
      }

      strncpy(t[idx].entries[cursor[idx]].key, buffer + offset, KEY_LEN);
      // copy value
      fgets(buffer, sizeof(buffer), fp);
      strncpy(t[idx].entries[cursor[idx]].value, buffer, VALUE_LEN);
      cursor[idx]++;
      insert_total_num++;
    } else if (!strcmp(buffer, "read\n")) {
      // read key
      fgets(buffer, sizeof(buffer), fp);
      uint64_t hash_key = xxhash(buffer, KEY_LEN, HASH_SEED);
      const uint32_t idx = hash_key% t_size;

      // check 对应线程任务队列是否已满
      if (t[idx].size == cursor[idx]) {
        t[idx].entries = static_cast<op_entry *>(
            realloc(t[idx].entries, sizeof(struct op_entry) * t[idx].size * 2));
        t[idx].size *= 2;
      }

      // set op type
      t[idx].entries[cursor[idx]].op = READ;
      // copy key
      int str_len = strlen(buffer) - 1;
      int offset = str_len - 20; // 取偏移量
      if(str_len < 20) {
        read_error_len++;
        // printf("[read %ld] ycsb origin key_length < 20 \n", read_error_len);
        continue;
      }

      strncpy(t[idx].entries[cursor[idx]].key, buffer + offset, KEY_LEN);
      cursor[idx]++;
      read_total_num++;
    }
  }

  // 输出结果
  std::cout<<"read error num:"<<read_error_len<<std::endl;
  std::cout<<"insert error num:"<<insert_error_len<<std::endl;
  std::cout<<"read total num:"<<read_total_num<<std::endl;
  std::cout<<"insert total num:"<<insert_total_num<<std::endl;

  // 将结果输出到文件
  std::ofstream file;
  file.open("../src/out.txt",std::ios::app);
  file<<"workload:"<<workload.c_str()<<std::endl;
  file<<"thread_num:"<<std::to_string(thread_num)<<std::endl;
  file<<"mode:"<<mode.c_str()<<std::endl;
  file<<"ycsb read keylen error:"<<read_error_len<<", ycsb insert keylen error:"<<insert_error_len<<", read total num:"<<read_total_num<<", insert total num:"<<insert_total_num<<std::endl;
  file.close();




  for (int i = 0; i < t_size; i++) {
    t[i].entries = static_cast<op_entry *>(
        realloc(t[i].entries, cursor[i] * sizeof(op_entry)));
    t[i].size = cursor[i];
  }

  free(cursor);
  fclose(fp);
}


void *running(void *in)
{
  thread_input *input = (thread_input *) in;
  set_affinity(input->idx);
  for (int i = 0; i < input->t->size; i++) {
    if (input->t->entries[i].op == READ) {
      //这里查找出key和value即可
      string_key*key=reinterpret_cast<string_key*>(alloca(sizeof(string_key)+KEY_LEN));
      key->length=KEY_LEN;
      memcpy(key->key,input->t->entries[i].key,sizeof(input->t->entries[i].key));
      const char*value=input->index->Get(key);
    } else if (input->t->entries[i].op == INSERT) {
      //得到key的地址
      PMEMoid  kptr;
      PMEMoid  vptr;
      Allocator::Allocate(&kptr,kCacheLineSize,KEY_LEN+sizeof (string_key),NULL,NULL);
      string_key*key= static_cast<string_key*>(pmemobj_direct(kptr));

      Allocator::Allocate(&vptr,kCacheLineSize,VALUE_LEN,NULL,NULL);
      char*value=static_cast<char *>(pmemobj_direct(vptr));
      key->length=KEY_LEN;
      memcpy(key->key,input->t->entries[i].key,sizeof(input->t->entries[i].key));
      memcpy(value,input->t->entries[i].key,sizeof(input->t->entries[i].value));
      //持久化即可
      Allocator::Persist(key,KEY_LEN+sizeof (string_key));
      Allocator::Persist(value,VALUE_LEN);
      //这里我就只插入key和value即可,强转
      input->index->Insert(key,value,true);
    }
  }
}


int main(int argc, char *argv[])
{
  if(argc < 5){
    std::cout<<"argc num must be set 3 numbers: \neg:ycsb_multi_thread_test workload_type thread_num default_path mode";
  }
  workload = argv[1];//负载
  thread_num = atoi(argv[2]);
  default_path = argv[3];
  mode = argv[4];

  const uint32_t task_size = 16;

  std::cout<<"start get data"<<std::endl;
  task t[thread_num];
  for (int i = 0; i < thread_num; i++) {
    t[i].size = task_size;
    t[i].entries =
        static_cast<op_entry *>(malloc(sizeof(op_entry) * task_size));
    memset(t[i].entries, 0, sizeof(struct op_entry) * task_size);
  }

  // 初始化任务队列
  read_input_files(default_path, t, thread_num);
   for(int i=0; i<thread_num; i++) {
     std::cout<<"t["<<i<<"] size:"<<t[i].size<<std::endl;
   }
  std::cout<<"finish get data"<<std::endl;

  std::cout<<"start create pool"<<std::endl;
  //初始化dash
  Hash<string_key*>*index;
  Allocator::Initialize(pool_name,pool_size);
  //一个段 64normal+2stash
  //这里就测试对不对，假设1024个段
  int seg_num=1024;
  index= reinterpret_cast<Hash<string_key*>*>(
      Allocator::GetRoot(sizeof(extendible::Finger_EH<string_key *>)));
  new (index) extendible::Finger_EH<string_key*>(seg_num, Allocator::Get()->pm_pool_);

  std::cout<<"create pool success"<<std::endl;


  // --------------------------- start multi logic here --------------------------- //
  struct timeval start, end;
  gettimeofday(&start, NULL); // 这个函数可以精确到微秒级

  pthread_t pt[thread_num];
  thread_input t_in[thread_num];

  // 创建多线程，执行任务
  for (int i = 0; i < thread_num; i++) {
    t_in[i].index=index;
    t_in[i].t = &t[i];
    t_in[i].idx=i;
    pthread_create(&pt[i], NULL, running, (void *) &t_in[i]);
  }

  // 等待子线程结束
  for (int i = 0; i < thread_num; i++) {
    pthread_join(pt[i], NULL);
  }

  gettimeofday(&end, NULL);
  int timeuse = 1000000 * ( end.tv_sec - start.tv_sec ) + end.tv_usec - start.tv_usec;
  std::cout<<"time:"<<timeuse<<"us"<<std::endl;

  for (int i = 0; i < thread_num; i++) {
    free(t[i].entries);
  }
  printf("end");


  // 将用时结果输出到文件
  std::ofstream file;
  file.open("../src/out.txt",std::ios::app);
  file<<"timeuse(us):"<<timeuse<<std::endl;
  file<<"------------------------------------------------------------"<<std::endl;
  file.close();


  return 0;
}





