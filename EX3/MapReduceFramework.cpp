//
// Created by mona.zoabi on 5/25/23.
//

#include "MapReduceFramework.h"
#include "Barrier.h"
#include <iostream>
#include "atomic"
#include <cstdint>
#include "algorithm"

#define ERR_CREATE std::cerr<< "system error: pthread_create FAILED!! "<<std::endl;
#define ERR_block std::cerr<< "system error: pthread_BLOCK / UNBLOCK FAILED!!"<<std::endl;

//
//class Comp_K2
//{
// public:
//  bool operator()(const K2 *first, const K2 *second)
//  {
//    return *(first) < *(second);
//  }
//};


void *init_func(void * arg);
class Thread_context;

//std::vector <IntermediateVec> shuffled_vec;

struct  job_handler {
    std::vector <IntermediateVec> shuffled_vec;
    pthread_t *threads_array;
    Thread_context *all_thread_contexts;

    MapReduceClient &_client;
    InputVec &_inputVec;
    OutputVec &_outputVec;
    int _multiThreadLevel;
    JobState *job_state{};
    Barrier* barrier;

//**************** INITIALIZE ATOMIC COUNTERS **********************//

    std::atomic<int>  atomic_map_counter; //next input index
    std::atomic<int> num_pairs;
    std::atomic<int> num_reduce;
    std::atomic<uint64_t> proccess_counter;
    pthread_mutex_t count_mutex2 = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;
    bool done = false;

    //-----------------------------------------------------------------------------------------------
    //constructor for job_handler
    job_handler(Thread_context* array_contexts,const MapReduceClient &client,
                const InputVec &input, OutputVec &out,
                int multi):
        _client(const_cast<MapReduceClient&>(client)),
        _outputVec(out),
        _inputVec(const_cast<InputVec &>(input)),
        _multiThreadLevel(multi)
    {
      uint64_t my_mask = 0x7fffffff;
      //initialize the JobState fields;
      all_thread_contexts = array_contexts;
      barrier= new Barrier (_multiThreadLevel);
      done = false;
      threads_array = new pthread_t[_multiThreadLevel];
      num_pairs  = 0;
      atomic_map_counter = 0;
      num_reduce = 0;
      proccess_counter =  0;
      proccess_counter = (0 & (~my_mask << (unsigned)31)) |
                         ( ((uint64_t) _inputVec.size() )<< (unsigned)31);
    }
};

struct Thread_context {

 public:
  int thread_id;
  IntermediateVec intermedate_vec;
  job_handler *my_job;

};



void getJobState(JobHandle job, JobState *state)
{
  auto *tc = static_cast<job_handler *>(job);

  auto flag = pthread_mutex_lock(&tc->count_mutex); //lock
  if (flag != 0)
  {
      ERR_block
      pthread_mutex_destroy(&tc->count_mutex);
      exit(1);
  }

  auto final1 = (tc ->proccess_counter >> (unsigned)31) &
          (unsigned)(0x7fffffff);
  auto final2 =(tc->proccess_counter)& (0x7fffffff);
  auto down = (float)final1;
  auto  up = final2 ;
  state->percentage = (up/down) *100;
  state->stage = (stage_t)((uint64_t)((tc->proccess_counter.load()
          &
          (uint64_t)((uint64_t)3 << (uint64_t)62)) >> (uint64_t)62));

  flag = pthread_mutex_unlock(&tc->count_mutex); //lock
    if (flag != 0)
    {
        ERR_block
        pthread_mutex_destroy(&tc->count_mutex);
        exit(1);
    }
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{


  auto *all_thread_contexts = new Thread_context[multiThreadLevel];
  job_handler * job = new job_handler(all_thread_contexts,client,
                                      inputVec, outputVec,
                                      multiThreadLevel);

  for (int i = 0; i < multiThreadLevel; ++i)
  {
    job->all_thread_contexts[i].thread_id = i;
    job->all_thread_contexts[i].my_job = job;
    int res = pthread_create(&job->threads_array [i],nullptr,
                             init_func,&job->all_thread_contexts[i]);

    if(res != 0 )
    {
        ERR_CREATE
      exit(1);
    }
  }

  return job;

}

//#################### HELPERS !! #####################################

void set_proccess_counter_thread(Thread_context *tc, uint64_t val)
{
  tc->my_job->proccess_counter = val;
}

void set_proccess_counter_job(job_handler *tc, uint64_t val)
{

  tc->proccess_counter = val;
}

void increment_counter(Thread_context * tc)
{
  tc->my_job->proccess_counter++;
}



bool Comparator(const IntermediatePair &first, const IntermediatePair &second)
{
  return *(first.first) < *(second.first);
}


void sort_phase(void * arg)
{

  auto tc = (Thread_context*) arg;
  std::sort(tc->intermedate_vec.begin(),tc->intermedate_vec.end(),
            Comparator);
}

//###########################PHASES !! #######################################

void Map_phase(void *arg)
{
  auto *tc = (Thread_context *) arg;
  uint64_t bit_num;
  auto limit = (tc ->my_job->proccess_counter.load() >>
                                 (unsigned)31) & (unsigned)(0x7fffffff);

  while ((bit_num= tc->my_job->atomic_map_counter++)< limit)
  {
    InputPair currPair = tc->my_job->_inputVec[bit_num];
    tc->my_job->_client.map(currPair.first, currPair.second, tc);
    increment_counter(tc);
  }
}



void shuffle_helper(void *arg,IntermediatePair *max_k2)
{
  auto *tc = static_cast<job_handler *>(arg);
  std::vector<IntermediatePair> new_vec;
  int i = 0 ;
  while (i < tc->_multiThreadLevel)
  {
    while ((tc->all_thread_contexts[i].intermedate_vec.size() != 0)
           &&
           !Comparator (tc->all_thread_contexts[i].intermedate_vec.back(),*max_k2)
           &&
           !Comparator (*max_k2,tc->all_thread_contexts[i].intermedate_vec.back()))
    {
      new_vec.push_back(tc->all_thread_contexts[i].intermedate_vec.back());
      tc->all_thread_contexts[i].intermedate_vec.pop_back();
      tc->proccess_counter++;
    }
    i++;
  }

  tc->shuffled_vec.push_back(new_vec);
  tc->num_reduce++;


}


void shuffle_phase(void *arg)
{

  auto *tc = static_cast<job_handler *>(arg);

  auto x= (((uint64_t) tc->num_pairs << (uint64_t)31)
          + ((uint64_t)2 << (uint64_t)62));

  set_proccess_counter_job(tc,x);

  auto limit = (tc->proccess_counter >> (unsigned)31)
          & (unsigned)(0x7fffffff);

  auto limit2 = (tc->proccess_counter)& (0x7fffffff);

  while (limit != limit2)
  {
    int i = 0;
    IntermediatePair *maximum = nullptr;
    while (i < tc->_multiThreadLevel)
    {

      if (tc->all_thread_contexts[i].intermedate_vec.size() != 0)
      {
    IntermediatePair pair = tc->all_thread_contexts[i].intermedate_vec.back();
        if (maximum == nullptr || Comparator (*maximum,pair))
//        if (maximum == nullptr || *maximum->first < *pair.first)
        {
          maximum = &(tc->all_thread_contexts[i].intermedate_vec).back();
        }
      }
      i++;
    }

    shuffle_helper(tc, maximum);

    limit = (tc->proccess_counter >> (unsigned)31) & (unsigned)(0x7fffffff);
    limit2 = (tc->proccess_counter)& (0x7fffffff);
  }

}


//################ REDUCE ############################

void reduce_phase(void *arg)
{

  auto tc = (Thread_context*) arg;
//  int i =0;
  while (tc->my_job->shuffled_vec.size() != 0)
  {
    auto flag = pthread_mutex_lock(&tc->my_job->count_mutex2);  //lock
    if (flag != 0)
    {
        ERR_block
        pthread_mutex_destroy(&tc->my_job->count_mutex2);
        exit(1);

    }


    if(tc->my_job->shuffled_vec.size()==0)
    {

      flag = pthread_mutex_unlock(&tc->my_job->count_mutex2);     //unlock
      if (flag != 0)
      { ERR_block
      pthread_mutex_destroy(&tc->my_job->count_mutex2);
      exit(1);
      }//check unblock
      break;
    }

    IntermediateVec current_vec = tc->my_job->shuffled_vec.back();
    tc->my_job->shuffled_vec.pop_back();
    flag = pthread_mutex_unlock(&tc->my_job->count_mutex2);

    if (flag != 0)
    {
    ERR_block
    pthread_mutex_destroy(&tc->my_job->count_mutex2);
    exit(1);
    }
    tc->my_job->_client.reduce(&current_vec, tc->my_job);
  }


}


void *init_func(void * arg)
{
  auto *tc = static_cast<Thread_context *>(arg);

  auto x = (~( (uint64_t)3 << (uint64_t)62) &
        tc->my_job->proccess_counter.load()) +((uint64_t)1 << (uint64_t)62);

  set_proccess_counter_thread(tc, x);

  Map_phase(tc);
  sort_phase(tc);

  tc->my_job->barrier->barrier();

  if (!tc->thread_id)
  {
    shuffle_phase(tc->my_job);
    x = (((uint64_t) tc->my_job->num_reduce << (uint64_t)31) +
         ((uint64_t)3 << (uint64_t)62));
    set_proccess_counter_thread(tc, x);

  }
  tc->my_job->barrier->barrier();
  reduce_phase(tc);

  return nullptr;
}

//######################## EMITS ##############################

void emit2(K2 *key, V2 *value, void *context)
{
  auto *tc = static_cast<Thread_context *>(context);
  tc->my_job->num_pairs++;
  tc->intermedate_vec.push_back(IntermediatePair (key,value));
}


void emit3(K3 *key, V3 *value, void *context)
{
  auto *tc = static_cast<job_handler *>(context);

  auto flag = pthread_mutex_lock(&tc->count_mutex2);
    if (flag != 0)
    {
        ERR_block
        pthread_mutex_destroy(&tc->count_mutex2);
        exit(1);
    }
  tc->_outputVec.push_back(OutputPair (key,value));
  tc->proccess_counter ++;
  auto flag2 = pthread_mutex_unlock(&tc->count_mutex2);
    if (flag2 != 0)
    {
        ERR_block
        pthread_mutex_destroy(&tc->count_mutex2);
        exit(1);
    }
}


// ########################## FINALLY DONE!!!!!  ############################
void waitForJob(JobHandle job)
{
  auto *tc = static_cast<job_handler *>(job);

  auto flag = pthread_mutex_lock(&tc->count_mutex);
    if (flag != 0)
    {
        ERR_block
        pthread_mutex_destroy(&tc->count_mutex);
        exit(1);
    }
  if(tc->done == false)
  {
    tc->done = true;
    int counter = 0;
    while (counter < tc->_multiThreadLevel)
    {
      auto check =  pthread_join(tc->threads_array[counter],
                                 nullptr);

      if (check != 0) {
        std::cerr<<"system error: pthread_create() error"<<std::endl;
        exit(1);
      }
      counter++;
    }
  }
  flag = pthread_mutex_unlock(&tc->count_mutex);
    if (flag != 0)
    {
        ERR_block
        pthread_mutex_destroy(&tc->count_mutex);
        exit(1);
    }
}



void closeJobHandle(JobHandle job)
{
  auto *tc = static_cast<job_handler *>(job);
  while (true)
  {
    if (tc->done == false)
      waitForJob(job);
    break;
  }

//  FREE ALL RESOURCES!!!!!!!
  delete[] tc->threads_array;
  delete tc->barrier;
  delete [] tc->all_thread_contexts;
  delete tc;

}