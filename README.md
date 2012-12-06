
# ThreadPool

Complementary class to the [ThreadPoolExecutor](http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/ThreadPoolExecutor.html)
that provides a bounded version of
[newCachedThreadPool](http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/Executors.html#newCachedThreadPool\(\)).

This class behaves differently to the ThreadPoolExecutor in that threads are created in preference to jobs being
queued. However, jobs are queued AFTER the maximum number of threads has been reached. These differences in
 behaviour makes the ThreadPool more suitable for relatively long lasting jobs, such as blocking IO for example.

This code is MIT licenced. At the moment it is not pushed to a Maven repository but I plan to do this once test coverage
improves and I've used it a bit more.
