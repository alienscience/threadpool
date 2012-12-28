package uk.org.alienscience.threadpool;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests of a cached thread pool
 */
public class CachedThreadPoolTests {

    private static final long timeout = 200;

    // Latches used for synchronization
    volatile CountDownLatch start;
    volatile CountDownLatch finish;

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

    @Before
    public void setUp() {
        // Create a cached thread pool with a small keep alive timeout
        pool = new ThreadPool(2,
                timeout, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<Runnable>(),
                Executors.defaultThreadFactory(),
                ThreadPool.defaultRejectionPolicy);
    }

    @Test
    public void testIdleTimeout() throws InterruptedException {
        start = new CountDownLatch(1);
        finish = new CountDownLatch(2);

        // Submit a job
        pool.submit(new Job());

        // Run the job
        start.countDown();

        // Wait for an idle timeout
        finish.await(timeout * 4, TimeUnit.MILLISECONDS);

        // Confirm that no threads are running
        assertEquals(0, pool.getNumThreads());
    }
}
