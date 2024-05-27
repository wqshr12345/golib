package common

/*
#define _GNU_SOURCE
#include <sched.h>
#include <pthread.h>

int lock_thread(int cpuid) {
  pthread_t tid;
  cpu_set_t cpuset;

  tid = pthread_self();
  CPU_ZERO(&cpuset);
  CPU_SET(cpuid, &cpuset);
  return  pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
}

pthread_t current_thread_id() {
  pthread_t tid;

  tid = pthread_self();

  return tid;
}
*/
import "C"

import (
	"fmt"
	"runtime"
	"syscall"
	"time"
)

// SetAffinity 设置CPU绑定
func SetAffinity(cpuID int) (uint64, error) {
	runtime.LockOSThread()
	ret := C.lock_thread(C.int(cpuID))
	tid := uint64(C.ulong(C.current_thread_id()))
	if ret > 0 {
		return 0, fmt.Errorf("set cpu core affinity failed with return code %d", ret)
	}
	return tid, nil
}

func GetRusage() (*syscall.Rusage, error) {
	var usage syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	if err != nil {
		return nil, err
	}
	return &usage, nil
}

// cpuTimeDiff returns the difference in user and system CPU time between two Rusage values
func CpuTimeDiff(start, end *syscall.Rusage) (userTime, systemTime time.Duration) {
	userTime = time.Duration(end.Utime.Sec-start.Utime.Sec)*time.Second + time.Duration(end.Utime.Usec-start.Utime.Usec)*time.Microsecond
	systemTime = time.Duration(end.Stime.Sec-start.Stime.Sec)*time.Second + time.Duration(end.Stime.Usec-start.Stime.Usec)*time.Microsecond
	return
}
