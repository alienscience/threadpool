package uk.org.alienscience.threadpool;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Tests of the ThreadPool rejection policy
 */
public class RejectionPolicyTests {

    private CountDownLatch start;
    private CountDownLatch finish;

    // Helper class that is controlled by a single latch
    private class Job implements Runnable {

        @Override
        public void run() {
            try {
                if (start != null) start.await();
            } catch (InterruptedException e) {
                // Ignore
            }
            finish.countDown();
        }
    }

    @Before
    public void setup() {
        start = new CountDownLatch(1);
        finish = new CountDownLatch(1);
    }

    @Test(expected = RejectedExecutionException.class)
    public void testAbort() {

        // Create a single threaded pool which will abort when the second job is submitted
        ExecutorService pool = new ThreadPool(1,
                60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                Executors.defaultThreadFactory(),
                ThreadPool.Policy.ABORT);

        // Submit a job
        pool.submit(new Job());

        // Expect an abort on the 2nd submit
        pool.submit(new Job());
    }

    @Test
    public void testCallerRuns() {

        // Create a single threaded pool which will abort when the second job is submitted
        ExecutorService pool = new ThreadPool(1,
                60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                Executors.defaultThreadFactory(),
                ThreadPool.Policy.CALLER_RUNS);

        // Submit a job
        pool.submit(new Job());

        final ThreadLocal<Boolean> isThisThread = new ThreadLocal<Boolean>();

        // Expect the next job to be run by this thread
        pool.submit(new Runnable() {
            @Override
            public void run() {
                // Signal the other thread in the pool to start
                start.countDown();
                try {
                    // Wait for the other thread to finish
                    finish.await(10, TimeUnit.SECONDS);

                    // Indicate that this thread has done something
                    isThisThread.set(true);
                } catch (InterruptedException e) {
                    // Ignored
                }

            }
        });

        // When the submit is finished this thread should have done something
        Boolean done = isThisThread.get();
        assertNotNull(done);
        assertEquals(done, Boolean.TRUE);
    }

    @Test
    public void testDiscard() throws InterruptedException {

        // Create a single threaded pool which will abort when the second job is submitted
        ExecutorService pool = new ThreadPool(1,
                60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                Executors.defaultThreadFactory(),
                ThreadPool.Policy.DISCARD);

        // Submit a job
        pool.submit(new Job());

        // Expect the next job not to be run
        pool.submit(new Runnable() {
            @Override
            public void run() {
                start.countDown();
                try {
                    finish.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        });

        // Make sure the second job was not run
        assertFalse(finish.await(100, TimeUnit.MILLISECONDS));
    }

    @Test(expected = RejectedExecutionException.class)
    public void testDiscardOldestException() throws InterruptedException {

        // Create a single threaded pool which will throw an exception when the second job is submitted
        // because the the 2nd job cannot be put onto the synchronous queue
        ExecutorService pool = new ThreadPool(1,
                60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                Executors.defaultThreadFactory(),
                ThreadPool.Policy.DISCARD_OLDEST);

        // Submit two jobs
        pool.submit(new Job());
        pool.submit(new Job());
    }

    @Test
    public void testDiscardOldest() throws InterruptedException {

        // Create a single threaded pool which will throw an exception when the second job is submitted
        ExecutorService pool = new ThreadPool(1,
                60l, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(1),
                Executors.defaultThreadFactory(),
                ThreadPool.Policy.DISCARD_OLDEST);

        // Submit a job to take the one available thread
        pool.submit(new Job());

        // Have a latch to indicate that the 2nd job finished
        final CountDownLatch secondFinish = new CountDownLatch(1);

        // Submit the second job
        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    start.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        });

        // Have a latch to indicate that the 3rd job finished
        final CountDownLatch thirdFinish = new CountDownLatch(1);

        // Expect the next job to run
        pool.submit(new Runnable() {
            @Override
            public void run() {
                thirdFinish.countDown();
            }
        });

        start.countDown();

        // Make sure the first job was run
        assertTrue(finish.await(10, TimeUnit.SECONDS));

        // The third job should have been run
        assertTrue(thirdFinish.await(10, TimeUnit.SECONDS));

        // The second job should not have been run
        assertFalse(secondFinish.await(100, TimeUnit.MILLISECONDS));
    }
}
