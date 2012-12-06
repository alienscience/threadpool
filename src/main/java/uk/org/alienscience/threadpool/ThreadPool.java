package uk.org.alienscience.threadpool;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An alternative to {@link ThreadPoolExecutor} that queues <i>after</i> hitting thread limits and also
 * provides a bounded CachedThreadPool.
 */
public class ThreadPool extends AbstractExecutorService {

    private static final RejectedHandler defaultRejectionPolicy = Policy.ABORT;

    // Configuration
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final ThreadFactory threadFactory;

    private final int maximumPoolSize;
    private final RejectedHandler rejectedHandler;
    private final BlockingQueue<Runnable> workQueue;

    // State that does not require locking
    private final SynchronousQueue<Runnable> idleChannel;
    private final Set<WorkerThread> workers;
    private final AtomicBoolean shutdown;
    private final CountDownLatch terminate;

    // A job that indicates that a worker should be shut down
    enum Signal implements Runnable {
        SHUTDOWN_WORKER;
        @Override public void run() { }
    }

    /**
     * Created a bounded version of a {@link java.util.concurrent.Executors#newCachedThreadPool()}
     * that queues jobs when the maximumPoolSize is reached.
     */
    public static ExecutorService newCachedThreadPool(int maximumPoolSize) {
        return new ThreadPool(maximumPoolSize, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                Executors.defaultThreadFactory(),
                defaultRejectionPolicy);
    }

    /**
     * Created a bounded version of a {@link java.util.concurrent.Executors#newCachedThreadPool()}
     * that queues jobs when the maximumPoolSize is reached.
     */
    public static ExecutorService newCachedThreadPool(int maximumPoolSize, ThreadFactory threadFactory) {
        return new ThreadPool(maximumPoolSize, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                threadFactory,
                defaultRejectionPolicy);
    }

    /**
     * Create a thread pool.
     *
     * @param maximumPoolSize The maximum number of threads in the pool
     * @param keepAliveTime The time a thread is idle before it is removed from the pool
     * @param timeUnit The time unit of keepAliveTime
     * @param workQueue A blocking queue that will be used when maximumPoolSize is reached
     * @param threadFactory A thread factory that will be used to create new threads
     * @param rejectedHandler The policy to follow when no more jobs can be added
     */
    public ThreadPool(int maximumPoolSize,
                      long keepAliveTime,
                      TimeUnit timeUnit,
                      BlockingQueue<Runnable> workQueue,
                      ThreadFactory threadFactory,
                      RejectedHandler rejectedHandler) {
        this.maximumPoolSize = maximumPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.workQueue = workQueue;
        this.threadFactory = threadFactory;
        this.rejectedHandler = rejectedHandler;

        this.idleChannel = new SynchronousQueue<Runnable>();
        this.workers = Collections.newSetFromMap(new ConcurrentHashMap<WorkerThread, Boolean>(maximumPoolSize));
        this.shutdown = new AtomicBoolean(false);
        this.terminate = new CountDownLatch(1);
    }


    /**
     * Create a thread pool with the same configuration as the given thread pool.
     * This constructor is only intended for testing because it will share the same
     * work queue and thread factory
     * @param pool The pool to copy the configuration from
     */
    ThreadPool(ThreadPool pool) {
        this(pool.maximumPoolSize,
                pool.keepAliveTime,
                pool.timeUnit,
                pool.workQueue,
                pool.threadFactory,
                pool.rejectedHandler);
    }

    // ---------- A worker thread that does the actual execution --------------
    private class WorkerThread implements Runnable {
        Runnable command;
        private Thread thread;


        public WorkerThread(Runnable command) {
            this.command = command;
        }

        public void startThread(ThreadFactory threadFactory) {
            thread = threadFactory.newThread(this);
            thread.start();
        }

        public void interruptThread() {
            thread.interrupt();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    while (command != null) {
                        // Run the command
                        command.run();

                        // Check the work queue for more jobs
                        command = workQueue.poll();
                    }

                    // Don't check the idle channel on shutdown
                    if (shutdown.get()) break;

                    command = idleChannel.poll(keepAliveTime, timeUnit);

                    // Check for timeout
                    if (command == null)  break;

                    // Check for shutdown
                    if (command == Signal.SHUTDOWN_WORKER) break;
                }
            } catch (InterruptedException e) {
                // Fall through and exit
            }

            // If this point is reached the worker is exiting
            workers.remove(this);

            // Check for thread pool termination
            if (shutdown.get() && workers.isEmpty()) terminate.countDown();
        }
    }

    // ---------- ThreadPool --------------------------------------------------

    /**
     * Executes the given job.
     * @param command The job to execute
     */
    public void execute(Runnable command) {
        // Don't accept jobs during shutdown
        if (shutdown.get()) return;

        // Is there a worker free?
        if (idleChannel.offer(command)) return;

        // Can a new worker be created?
        if (createNewWorker(command)) return;

        // Can the command be added to the work queue?
        if (workQueue.offer(command)) return;

        // If this point is reached, the command cannot be run
        reject(command);
    }

    // Create a new worker thread, returns true on success
    // Called with lock obtained
    private boolean createNewWorker(Runnable command) {

        // Can a new thread be created?
        int numThreads = workers.size();
        if (numThreads >= maximumPoolSize) return false;

        // Create the new worker
        WorkerThread worker = new WorkerThread(command);
        workers.add(worker);
        worker.startThread(threadFactory);
        return true;
    }

    // Reject the given job
    private void reject(Runnable command) {
        rejectedHandler.rejectedExecution(command, this);
    }

    /**
     * Starts an orderly shutdown where all submitted jobs are executed before termination.
     * Returns quickly before the shutdown is completed.
     */
    @Override
    public void shutdown() {
        // Indicate shutdown
        shutdown.set(true);

        // Shutdown all the idle workers
        while (idleChannel.offer(Signal.SHUTDOWN_WORKER)) { }
    }

    /**
     * Shutdowns as quickly as possible returning all the jobs on the work queue.
     * @return The jobs that were in the work queue
     */
    @Override
    public List<Runnable> shutdownNow() {
        // Indicate shutdown
        shutdown.set(true);

        ArrayList<Runnable> ret = new ArrayList<Runnable>();

        workQueue.drainTo(ret);
        for (WorkerThread w : workers) {
                w.interruptThread();
        }

        return ret;
    }

    @Override
    public boolean isShutdown() {
       return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return (terminate.getCount() == 0);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminate.await(timeout, timeUnit);
    }

    //---------- Getters -------------------------

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public int getQueueSize() {
        return workQueue.size();
    }

    public int getNumThreads() {
        return workers.size();
    }

    //---------- RejectedHandlers ----------------

    enum Policy implements RejectedHandler {
        /**
         * Throws a {@link RejectedExecutionException}
         */
        ABORT {
            @Override
            public void rejectedExecution(Runnable command, ThreadPool threadPool) {
                throw new RejectedExecutionException("Thread pool too busy");
            }
        },
        /**
         * Runs the job in the calling thread
         */
        CALLER_RUNS {
            @Override
            public void rejectedExecution(Runnable command, ThreadPool threadPool) {
                command.run();
            }
        },
        /**
         * Discard the job
         */
        DISCARD {
            @Override
            public void rejectedExecution(Runnable command, ThreadPool threadPool) {
                // Do nothing
            }
        },
        /**
         * Discard the oldest job at the head of the queue
         */
        DISCARD_OLDEST {
            @Override
            public void rejectedExecution(Runnable command, ThreadPool threadPool) {
                threadPool.workQueue.poll();
                threadPool.execute(command);
            }
        }
    }

}
