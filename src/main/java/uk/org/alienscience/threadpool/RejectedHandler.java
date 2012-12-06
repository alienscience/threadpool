package uk.org.alienscience.threadpool;

/**
 * Interface that is called when a job cannot be submitted to a thread pool
 */
public interface RejectedHandler {
    /**
     * Called when a job cannot be submitted to a thread pool
     * @param command The job that was submitted
     * @param threadPool The thread pool that rejected the job
     */
    void rejectedExecution(Runnable command, ThreadPool threadPool);
}
