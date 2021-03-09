// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_PORT_THREAD_ANNOTATIONS_H_
#define STORAGE_LEVELDB_PORT_THREAD_ANNOTATIONS_H_

// Use Clang's thread safety analysis annotations when available. In other
// environments, the macros receive empty definitions.
// Usage documentation: https://clang.llvm.org/docs/ThreadSafetyAnalysis.html

#if !defined(THREAD_ANNOTATION_ATTRIBUTE__)

#if defined(__clang__)

#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x)  // no-op
#endif

#endif  // !defined(THREAD_ANNOTATION_ATTRIBUTE__)

// /////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////
// clang的线程安全分析模块 thread safety analysis，能对代码中潜在的竞争条件进行警告。
// 这种分析是完全静态的（即编译时进行），没有运行时的消耗。
// /////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////

// Mutex mu;
// int *p1            GUARDED_BY(mu);
// int *p2            PT_GUARDED_BY(mu);
// unique_ptr<int> p3 PT_GUARDED_BY(mu);

// void test() {
//   p1 = 0;             // Warning!

//   p2 = new int;       // OK.
//   *p2 = 42;           // Warning!

//   p3.reset(new int);  // OK.
//   *p3 = 42;           // Warning!
// }

// 用于对data member的保护
// 线程在read/write变量之前，必须先lock x
#ifndef GUARDED_BY
#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))
#endif

// 用于对指针、智能指针的保护，对指针本身没有限制，但是指针指向的数据受到给定功能的保护
#ifndef PT_GUARDED_BY
#define PT_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))
#endif


// Mutex m1;
// Mutex m2 ACQUIRED_AFTER(m1);

// // Alternative declaration
// // Mutex m2;
// // Mutex m1 ACQUIRED_BEFORE(m2);

// void foo() {
//   m2.Lock();
//   m1.Lock();  // Warning!  m2 must be acquired after m1.
//   m1.Unlock();
//   m2.Unlock();
// }

// 防止deadlock：对互斥量或其他锁的获取，必须在获取__VA_ARGS__之后
#ifndef ACQUIRED_AFTER
#define ACQUIRED_AFTER(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))
#endif

// 防止deadlock：对互斥量或其他锁的获取，必须在获取__VA_ARGS__之前
#ifndef ACQUIRED_BEFORE
#define ACQUIRED_BEFORE(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))
#endif

// 线程在调用x函数之前，必须先lock：__VA_ARGS__
#ifndef EXCLUSIVE_LOCKS_REQUIRED
#define EXCLUSIVE_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))
#endif

// 同上exclusive_locks_required，但是只需要获取对__VA_ARGS__的共享访问权
#ifndef SHARED_LOCKS_REQUIRED
#define SHARED_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_locks_required(__VA_ARGS__))
#endif

// 防止deadlock：在调用x函数之前，线程必须先释放锁：__VA_ARGS__
// 是可选的，如果没有声明locks_excluded，将不会出现warning
#ifndef LOCKS_EXCLUDED
#define LOCKS_EXCLUDED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))
#endif

// 用于声明函数的功能是返回x的引用，用于声明返回互斥量的getter方法
#ifndef LOCK_RETURNED
#define LOCK_RETURNED(x) THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))
#endif

// 声明该类的对象可以用作锁
#ifndef LOCKABLE
#define LOCKABLE THREAD_ANNOTATION_ATTRIBUTE__(lockable)
#endif

// 实现 RAII-style 的锁，在构造函数中获取锁，在析构函数中释放锁
#ifndef SCOPED_LOCKABLE
#define SCOPED_LOCKABLE THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)
#endif

// 声明函数在具有获取锁的能力，调用者不应该在进入函数之前就获取锁__VA_ARGS__，
// 而在退出时会获取锁
#ifndef EXCLUSIVE_LOCK_FUNCTION
#define EXCLUSIVE_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_lock_function(__VA_ARGS__))
#endif

// 同上exclusive_lock_function，但是这里会获取锁__VA_ARGS__的共享访问权
#ifndef SHARED_LOCK_FUNCTION
#define SHARED_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_lock_function(__VA_ARGS__))
#endif

#ifndef EXCLUSIVE_TRYLOCK_FUNCTION
#define EXCLUSIVE_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_trylock_function(__VA_ARGS__))
#endif

#ifndef SHARED_TRYLOCK_FUNCTION
#define SHARED_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_trylock_function(__VA_ARGS__))
#endif

// 声明函数具有释放锁的能力，调用函数之前需要已经获取锁，该函数离开后会释放锁
// 独占锁、共享锁均可以
#ifndef UNLOCK_FUNCTION
#define UNLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))
#endif

#ifndef NO_THREAD_SAFETY_ANALYSIS
#define NO_THREAD_SAFETY_ANALYSIS \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)
#endif

// 运行时检测线程是否已经获取到了锁，如果没有获取到函数将fail
#ifndef ASSERT_EXCLUSIVE_LOCK
#define ASSERT_EXCLUSIVE_LOCK(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_exclusive_lock(__VA_ARGS__))
#endif

// 同上assert_exclusive_lock
#ifndef ASSERT_SHARED_LOCK
#define ASSERT_SHARED_LOCK(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_lock(__VA_ARGS__))
#endif

#endif  // STORAGE_LEVELDB_PORT_THREAD_ANNOTATIONS_H_
