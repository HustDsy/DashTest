//
// Created by dsy on 2021/4/24.
//

#include <gflags/gflags.h>
#include <immintrin.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>

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
const size_t pool_size = 1024ul * 1024ul * 1024ul * 20ul;  // 200G
// Set Test Config
std::string workload = "workloade";
uint8_t thread_num = 72;
std::string mode = "run";
const char *default_path;

struct op_entry {
  int op;  // 0 read 1 insert
  char key[KEY_LEN];
  char value[VALUE_LEN];
};
/**
 * entries key-value
 * size
 */
struct task {
  op_entry *entries;
  uint32_t size;
};

struct thread_input {
  task *t;
  Hash<string_key *> *index;
  uint32_t idx;
};

/**
 * ��ȡ�����ļ�����װ���ݵ� input ��
 * @param path
 * @param input
 */
// ͳ��origin key lenС��20����������ռ�������ʱ����Ҫ�����ⲿ��������
long read_error_len = 0;
long insert_error_len = 0;
// ͳ�Ʋ���Ͷ�ȡ��ycsb��������
long read_total_num = 0;
long insert_total_num = 0;

/**
 * ��������RAM,PMEM��Socket0֮�ϣ��������ܹ�������ʾ
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

void read_input_files(const char *path, task *t,
                      uint8_t t_size)  // t_size���߳���
{
  char buffer[1000];
  FILE *fp = fopen(path, "r");
  if (!fp) return;

  //ĳ���̶߳��е�λ�ã�
  int *cursor = reinterpret_cast<int *>(calloc(t_size, sizeof(int)));

  while (!feof(fp)) {
    memset(buffer, 0, sizeof(buffer));
    fgets(buffer, sizeof(buffer), fp);  // �����˻��з�
    if (!strcmp(buffer, "insert\n")) {
      // read key
      fgets(buffer, sizeof(buffer), fp);
      uint64_t hash_key = xxhash(buffer, KEY_LEN, HASH_SEED);

      const uint32_t idx = hash_key % t_size;

      // check ��Ӧ�߳���������Ƿ�����
      if (t[idx].size == cursor[idx]) {
        t[idx].entries = reinterpret_cast<op_entry *>(
            realloc(t[idx].entries, sizeof(struct op_entry) * t[idx].size * 2));
        t[idx].size *= 2;
      }
      // set op type
      t[idx].entries[cursor[idx]].op = INSERT;

      int str_len =
          strlen(buffer) -
          1;  // ycsb�ļ���ÿһ������л��з���Ҫ����,����ͨ��strlen�����������ĳ���Ҫ����ʵ��ʾ�����+1
      int offset = str_len - 20;  // ȡƫ����
      if (str_len < 20) {
        insert_error_len++;
        // printf("[insert %ld] ycsb origin key_length < 20 \n",
        // insert_error_len);
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
      const uint32_t idx = hash_key % t_size;

      // check ��Ӧ�߳���������Ƿ�����
      if (t[idx].size == cursor[idx]) {
        t[idx].entries = reinterpret_cast<op_entry *>(
            realloc(t[idx].entries, sizeof(struct op_entry) * t[idx].size * 2));
        t[idx].size *= 2;
      }

      // set op type
      t[idx].entries[cursor[idx]].op = READ;
      // copy key
      int str_len = strlen(buffer) - 1;
      int offset = str_len - 20;  // ȡƫ����
      if (str_len < 20) {
        read_error_len++;
        // printf("[read %ld] ycsb origin key_length < 20 \n", read_error_len);
        continue;
      }

      strncpy(t[idx].entries[cursor[idx]].key, buffer + offset, KEY_LEN);
      cursor[idx]++;
      read_total_num++;
    }
  }

  // ������
  std::cout << "read error num:" << read_error_len << std::endl;
  std::cout << "insert error num:" << insert_error_len << std::endl;
  std::cout << "read total num:" << read_total_num << std::endl;
  std::cout << "insert total num:" << insert_total_num << std::endl;

  // �����������ļ�
  std::ofstream file;
  file.open("../src/out.txt", std::ios::app);
  file << "workload:" << workload.c_str() << std::endl;
  file << "thread_num:" << std::to_string(thread_num) << std::endl;
  file << "mode:" << mode.c_str() << std::endl;
  file << "ycsb read keylen error:" << read_error_len
       << ", ycsb insert keylen error:" << insert_error_len
       << ", read total num:" << read_total_num
       << ", insert total num:" << insert_total_num << std::endl;
  file.close();

  for (int i = 0; i < t_size; i++) {
    t[i].entries = reinterpret_cast<op_entry *>(
        realloc(t[i].entries, cursor[i] * sizeof(op_entry)));
    t[i].size = cursor[i];
  }

  free(cursor);
  fclose(fp);
}

void *running(void *in) {
  thread_input *input = (thread_input *)in;
  set_affinity(input->idx);
  string_key *key =
      reinterpret_cast<string_key *>(malloc(sizeof(string_key) + KEY_LEN));
  const char *value = NULL;
  key->length = KEY_LEN;

  for (int i = 0; i < input->t->size; i++) {
    if (input->t->entries[i].op == READ) {
      //������ҳ�key��value����
      memcpy(key->key, input->t->entries[i].key,
             sizeof(input->t->entries[i].key));
      value = input->index->Get(key, true);
    } else if (input->t->entries[i].op == INSERT) {
      //�õ�key�ĵ�ַ
      PMEMoid kptr;
      PMEMoid vptr;
      Allocator::Allocate(&kptr, kCacheLineSize, KEY_LEN + sizeof(string_key),
                          NULL, NULL);
      if (!OID_IS_NULL(kptr)) {
        string_key *key = reinterpret_cast<string_key *>(pmemobj_direct(kptr));

        Allocator::Allocate(&vptr, kCacheLineSize, VALUE_LEN, NULL, NULL);
        char *value = reinterpret_cast<char *>(pmemobj_direct(vptr));
        key->length = KEY_LEN;
        memcpy(key->key, input->t->entries[i].key,
               sizeof(input->t->entries[i].key));
        memcpy(value, input->t->entries[i].key,
               sizeof(input->t->entries[i].value));
        //�־û�����
        Allocator::Persist(key, KEY_LEN + sizeof(string_key));
        Allocator::Persist(value, VALUE_LEN);
        //�����Ҿ�ֻ����key��value����,ǿת
        input->index->Insert(key, value, true);
      }
    }
  }
  //std::cout << "t[" << input->idx << "]:" << find_fail << std::endl;
  free(key);
}



int main(int argc, char *argv[]) {
  if (argc < 5) {
    std::cout << "argc num must be set 3 numbers: \neg:ycsb_multi_thread_test "
                 "workload_type thread_num default_path mode";
  }
  workload = argv[1];  //����
  thread_num = atoi(argv[2]);
  default_path = argv[3];
  mode = argv[4];

  const uint32_t task_size = 16;

  std::cout << "start get data" << std::endl;
  task t[thread_num];
  for (int i = 0; i < thread_num; i++) {
    t[i].size = task_size;
    t[i].entries =
        reinterpret_cast<op_entry *>(malloc(sizeof(op_entry) * task_size));
    memset(t[i].entries, 0, sizeof(struct op_entry) * task_size);
  }

  // ��ʼ���������
  read_input_files(default_path, t, thread_num);
  for (int i = 0; i < thread_num; i++) {
    std::cout << "t[" << i << "] size:" << t[i].size << std::endl;
  }
  std::cout << "finish get data" << std::endl;

  std::cout << "start create pool" << std::endl;
  //��ʼ��dash
  Hash<string_key *> *index;
  bool file_exist = false;
  if (FileExists(pool_name)) file_exist = true;
  Allocator::Initialize(pool_name, pool_size);
  //һ���� 64normal+2stash
  //����Ͳ��ԶԲ��ԣ�����1024����
  // int seg_num = 52428;  // 2��19�η�
  int seg_num = 16384 * 4;
  index = reinterpret_cast<Hash<string_key *> *>(
      Allocator::GetRoot(sizeof(extendible::Finger_EH<string_key *>)));
  if (!file_exist) {
    new (index) extendible::Finger_EH<string_key *>(seg_num,
                                                    Allocator::Get()->pm_pool_);
  } else {
    new (index) extendible::Finger_EH<string_key *>();
  }

  std::cout << "create pool success" << std::endl;

  // --------------------------- start multi logic here
  // --------------------------- //
  struct timeval start, end;
  gettimeofday(&start, NULL);

  pthread_t pt[thread_num];
  thread_input t_in[thread_num];

  // �������̣߳�ִ������
  for (int i = 0; i < thread_num; i++) {
    t_in[i].index = index;
    t_in[i].t = &t[i];
    t_in[i].idx = i;
    pthread_create(&pt[i], NULL, running, (void *)&t_in[i]);
  }

  // �ȴ����߳̽���
  for (int i = 0; i < thread_num; i++) {
    pthread_join(pt[i], NULL);
  }

  gettimeofday(&end, NULL);
  long int timeuse =
      1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  std::cout << "time:" << timeuse << "us" << std::endl;

  for (int i = 0; i < thread_num; i++) {
    free(t[i].entries);
  }
  printf("end");

  // ����ʱ���������ļ�
  std::ofstream file;
  file.open("../src/out.txt", std::ios::app);
  file << "timeuse(us):" << timeuse << std::endl;
  file << "------------------------------------------------------------"
       << std::endl;
  file.close();

  return 0;
}
