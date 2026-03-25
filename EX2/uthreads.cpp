#include "uthreads.h"
#include <vector>
#include "algorithm"
#include <sys/time.h>
#include "map"
//#include <cstdio>
#include <iostream>
#include "Thread.h"
#include <csignal>
#include <csetjmp>
#define MSG std::cout<< "thread library error: wrong input!"<<std::endl;
#define SYSTEMERRORMSG std::cout<< "system error: you have error from the system" <<std::endl;
//INITIALIZE TIME
//####################################################
struct itimerval time2{};
sigset_t sig_set;
int total_quantums = 0;
int total_threads_num = 1;
//####################################################

#define BLOCK sigprocmask(SIG_BLOCK, &sig_set,NULL)
#define UNBLOCK sigprocmask(SIG_UNBLOCK, &sig_set,NULL)

//####################################################
//initiate 3 vectors, ready, blocked, all
std::vector<Thread *> ready_vector;
std::vector<Thread *> all_threads_vec;
std::vector<Thread *> blocked_vector;
std::map<Thread*, int> sleeping_map;
//#####################################################
Thread *running = nullptr;

void delete_all(const std::vector<Thread *>& to_delete)
{
  for (auto & i : to_delete)
    {
      delete i;
    }
}

void initialize_all_vector()
{
    for (int i = 0; i < MAX_THREAD_NUM; ++i)
    {
        all_threads_vec.push_back(nullptr);
    }
}

void initialize_time(int quantum_usecs)
{
    BLOCK;
    time2.it_interval.tv_sec = 0;    // following time intervals, seconds part
    time2.it_interval.tv_usec = quantum_usecs;    // following time intervals, microseconds part

    time2.it_value.tv_sec = 0;        // first time interval, seconds part
    time2.it_value.tv_usec = quantum_usecs;
  // first time interval, microseconds part
    UNBLOCK;
}
Thread* min_value_in_map(std::map<Thread*, int> my_map)
{
    Thread* my_thread = nullptr;
    int min_val = my_map.begin()->second;
    for(const auto& iter :my_map)
    {
        if( min_val > iter.second)
        {
            min_val = iter.second;
            my_thread = iter.first;
        }
    }
    return my_thread;
}
void scheduler(int signal)
{
    BLOCK;
    total_quantums ++;
    for(auto iter : sleeping_map)
    {
        if (iter.second == total_quantums)
        {
            iter.first->thread_status = "ready";
            ready_vector.push_back(iter.first);
            Thread* to_delete =  min_value_in_map(sleeping_map);
            sleeping_map.erase(to_delete);
        }
    }

    if (sigsetjmp(running->getEnv(), 1) != 0)
        {
            UNBLOCK;
            return;
        }
    if(running->thread_status =="running")
    {

        running->thread_status = "ready" ;
        ready_vector.push_back(running);

    }
    auto next_elem= ready_vector.begin();
    running = all_threads_vec[(*next_elem)->tid];

    running->thread_status = "running";
    ready_vector.erase(ready_vector.begin());
    running->quantum ++;
    UNBLOCK;
    siglongjmp(running->getEnv(), 1);
}


int uthread_init(int quantum_usecs) {
    if (quantum_usecs <= 0)
    {
        //printf("hh");
        MSG
        return -1;
    }

    struct sigaction sa = {nullptr};
    sa.sa_handler = &scheduler;
    if (sigaction(SIGVTALRM, &sa, nullptr) < 0) {
        SYSTEMERRORMSG
        exit(1);
    }

    if (sigemptyset(&sig_set) > 0)//initializes the signal set given by set to empty, with all signals excluded from the set.
    {
        SYSTEMERRORMSG
        exit(1);
    }
    sigaddset(&sig_set, SIGVTALRM); //add signal signum from set.

    initialize_time(quantum_usecs);
    if (setitimer(ITIMER_VIRTUAL, &time2, nullptr)<0)
        {
            SYSTEMERRORMSG
            exit(1);
        }

    auto *main_thread = new Thread(0, nullptr);
    initialize_all_vector();
    all_threads_vec[0] = main_thread;
    all_threads_vec[0]->thread_status = "running";
    //all_threads_vec.push_back(main_thread);
    running = main_thread;
    main_thread->quantum++;
    total_quantums++;
    return 0;  //success
    }


int uthread_spawn(thread_entry_point entry_point)
{
  BLOCK;
  int count = 0;
  if (entry_point == nullptr)
  {
      MSG
      return -1;
  }
  while (count < MAX_THREAD_NUM)
    {
      if(all_threads_vec[count] == nullptr)
        {

          auto *new_thread = new Thread (count, entry_point);
          all_threads_vec [count] = new_thread;
          //ready_vector [count] = new_thread;
            ready_vector.push_back(new_thread);
            total_threads_num ++;
          UNBLOCK;
          return count;

        }
        count ++;
    }
    UNBLOCK;
    MSG
    return -1;
}

void delete_elem(int flag, Thread * to_delete)
{
    if (flag ==0) //ready
    {

        for (auto iter = ready_vector.begin() ; iter != ready_vector.end();iter++)
        {
            if(*iter == to_delete)
            {
                ready_vector.erase(iter);
                break;
            }

        }
    }
    if (flag ==1) //blocked
    {

        for (auto iter = blocked_vector.begin() ; iter != blocked_vector.end();iter++)
        {
            if(*iter == to_delete)
            {
                blocked_vector.erase(iter);
                break;
            }

        }
    }
}




int uthread_terminate(int tid)
{
    BLOCK;
    if ((tid > MAX_THREAD_NUM )| (tid < 0))
      {
        UNBLOCK;
        MSG
        return -1;
      }
    if (tid == 0)
    {
        delete_all (all_threads_vec);
        UNBLOCK;
        exit(0);
      }
    Thread *thread_to_delete= all_threads_vec[tid];
    if (thread_to_delete == nullptr)
      {
        UNBLOCK;
        MSG
        return -1;
      }
    else if (running->tid == thread_to_delete->tid)
      {
        running->thread_status = "terminated";
        scheduler(1);
      }
    else if ((thread_to_delete->thread_status == "ready") )
      {
        int flag = 0; //ready
        delete_elem(flag,thread_to_delete);
        total_threads_num --;
      }
    else if ((thread_to_delete->thread_status == "blocked"))
    {
        int flag = 1; //blocked
        delete_elem(flag,thread_to_delete);
        total_threads_num --;
    }
    else if (thread_to_delete->thread_status == "sleep")
    {
       sleeping_map.erase(thread_to_delete);
       total_threads_num --;
    }
    delete thread_to_delete;
    all_threads_vec[tid] = nullptr;
    UNBLOCK;
    return 0;
}

int uthread_block(int tid)
{
    BLOCK;
    if (tid <= 0 or tid > MAX_THREAD_NUM)
    {
        UNBLOCK;
        MSG
        return -1;
    }
    Thread *thread_to_block= all_threads_vec[tid];
    if (tid ==uthread_get_tid()) //THIS FUNCTION RETURNS THE RUNNING THREAD TID
    {
        thread_to_block->thread_status = "blocked";
        blocked_vector.push_back (thread_to_block);
        scheduler(1);

    }
    else if (thread_to_block == nullptr)
    {
        UNBLOCK;
        MSG
        return -1;
    }
    else if (thread_to_block->thread_status == "ready")
        {
            thread_to_block->thread_status = "blocked";
            blocked_vector.push_back (thread_to_block);
            ready_vector.erase(std::remove (ready_vector.begin (),
            ready_vector.end (),thread_to_block),ready_vector.end ());
        }

    else if (thread_to_block->thread_status == "sleep")
    {
        thread_to_block->thread_status = "blocked";
        sleeping_map.erase(thread_to_block);
    }

    UNBLOCK;
    return 0;

    }


int uthread_resume(int tid)
{
    BLOCK;
    if ((tid < 0 )or (tid >= MAX_THREAD_NUM))
    {
        UNBLOCK;
        MSG
        return -1;
    }
    if( all_threads_vec[tid] == nullptr)
    {
        MSG
        return -1;
    }
    Thread *thread_to_resume= all_threads_vec[tid];

    if ((thread_to_resume->thread_status == "ready") |
    (thread_to_resume->thread_status == "running"))
    {
        UNBLOCK;
        return 0;
    }
     if (thread_to_resume->thread_status == "blocked")
     {
         ready_vector.push_back(thread_to_resume);
         blocked_vector.erase(std::remove(blocked_vector.begin(),
      blocked_vector.end(),thread_to_resume),
                              blocked_vector.end());
         thread_to_resume->thread_status = "ready";
     }
     UNBLOCK;
     return 0;

}


int uthread_sleep(int num_quantums)
{
    BLOCK;
    if ((num_quantums < 0 )| (running->tid == 0))
    {
        MSG
        return -1;
    }
    running->thread_status = "sleep";
    sleeping_map[running] = num_quantums + total_quantums;
    scheduler(1);
    UNBLOCK;
    return 0;
}


int uthread_get_tid()
{
  return running->tid;
}

int uthread_get_total_quantums()
{
  return total_quantums;
}

int uthread_get_quantums(int tid)
{
  if (tid > MAX_THREAD_NUM || tid < 0)
    {
      MSG
      return -1;
    }
  Thread *thread= all_threads_vec[tid];
  if (thread == nullptr)
    {
      MSG
      return -1;
    }
  return thread->quantum;
}