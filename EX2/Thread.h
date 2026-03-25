//
// Created by mona zoabi on 13/04/2023.
//
//
#ifndef _THREAD_H_
#define _THREAD_H_
#include "string"
#include "uthreads.h"
#include "csignal"
#include <csetjmp>
#include <cstdlib>
class Thread
{
 public:
  int tid;
  int quantum;
  thread_entry_point func;
  std::string thread_status;
  sigjmp_buf env{};
  char * stack_;


  Thread(int tid, thread_entry_point func);
  sigjmp_buf& getEnv();
  ~Thread();


};

#endif //_THREAD_H_
