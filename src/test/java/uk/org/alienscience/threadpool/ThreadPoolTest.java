package uk.org.alienscience.threadpool;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ThreadPool unit tests
 * TODO test pool is actually threading
 * TODO check test coverage
 */
@RunWith(Parameterized.class)
public class ThreadPoolTest {

    // Latches used for synchronization
    volatile CountDownLatch start;
    volatile CountDownLatch finish;

    // The thread pool that is being tested
    ThreadPool pool;

    // Helper class that is controlled by the two latches
    private class Job implements Runnable {

        @Override
        public void run() {
            try {
                if (start != null) start.await();
            } catch (InterruptedException e) {
                return;
            }
            finish.countDown();
        }
    }

    @Parameterized.Parameters
    public static Collection testParameters() {
        Object[][]data = new Object[][] {
                {ThreadPool.newCachedThreadPool(1)},
                {ThreadPool.newCachedThreadPool(2)},
                {ThreadPool.newCachedThreadPool(32)},
        };
        return Arrays.asList(data);
    }

    public ThreadPoolTest(ThreadPool pool) {
        this.pool = new ThreadPool(pool);
    }

    @Test
    public void testJobsAreSubmitted() throws InterruptedException {
        final int numJobs = 256;
        start = new CountDownLatch(1);
        finish = new CountDownLatch(numJobs);

        // Submit jobs
        for (int i = 0; i < numJobs; i++) {
            pool.submit(new Job());
        }

        // Check the thread count and queue size
        assertEquals(pool.getMaximumPoolSize(), pool.getNumThreads());
        assertEquals(numJobs - pool.getMaximumPoolSize(), pool.getQueueSize());

        // Run the jobs
        start.countDown();

        assertTrue(finish.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testMultiThreadedSubmit() throws InterruptedException {
        final int numThreads = 8;
        final int jobsPerThread = 256;
        final int totalJobs = numThreads * jobsPerThread;
        start = new CountDownLatch(1);
        finish = new CountDownLatch(totalJobs);

        // Submit jobs using different threads
        Executor submitPool = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
             submitPool.execute(new Runnable() {
                 @Override
                 public void run() {
                     for (int j = 0; j < jobsPerThread; j++) {
                         pool.submit(new Job());
                     }
                 }
             });
        }

        // Run the jobs
        start.countDown();

        // Wait for completion
        assertTrue(finish.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testShutdown() throws InterruptedException {
        final int numJobs = 256;
        start = new CountDownLatch(1);
        finish = new CountDownLatch(numJobs);

        // Submit jobs
        for (int i = 0; i < numJobs; i++) {
            pool.submit(new Job());
        }

        // Shutdown
        pool.shutdown();

        // No jobs should have been run
        assertEquals(numJobs, finish.getCount());

        // Start the jobs
        start.countDown();

        // Wait for completion
        assertTrue(finish.await(10, TimeUnit.SECONDS));

        // Check termination
        assertTrue(pool.awaitTermination(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testShutdownNow() throws InterruptedException {
        final int numJobs = 256;
        start = new CountDownLatch(1);
        finish = new CountDownLatch(numJobs);

        // Submit jobs
        for (int i = 0; i < numJobs; i++) {
            pool.submit(new Job());
        }

        // Shutdown now
        List<Runnable> jobs = pool.shutdownNow();

        // All the jobs should have been returned
        assertEquals(numJobs - pool.getMaximumPoolSize(), jobs.size());

        // No jobs should have been run
        assertEquals(numJobs, finish.getCount());

        // Check termination
        assertTrue(pool.awaitTermination(100, TimeUnit.MILLISECONDS));
    }
}
