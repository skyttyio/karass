package io.skytty.karass.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Partitions and runs tasks on a group of threads.
public class AsyncTaskRunner {

  public static class AsyncTaskException extends IOException {
    public AsyncTaskException(Throwable e) {
      super(e);
    }
  }

  public interface AsyncTask {
    void run() throws Exception;
  }

  public static final int DEFAULT_NUM_PARTITIONS = 4;
  public static final int DEFAULT_QUEUE_CAPACITY = 16;
  protected static final AsyncTask eof = () -> {};

  private Map<Integer, BlockingQueue<AsyncTask>> queues = new HashMap<>();
  protected int numPartitions = DEFAULT_NUM_PARTITIONS;
  protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
  private Throwable latestException = null;
  private boolean closed = false;
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  public AsyncTaskRunner(int numPartitions, int queueCapacity) {
    this.numPartitions = numPartitions;
    this.queueCapacity = queueCapacity;
  }

  public AsyncTaskRunner() {}

  public void submit(String key, AsyncTask task) {
    submit(key.hashCode() % numPartitions, task);
  }

  public void submit(int partition, AsyncTask task) {
    assertNotClosed();
    BlockingQueue<AsyncTask> queue = getQueue(partition);
    try {
      lock.readLock().lock();
      while (true) {
        try {
          queue.put(task);
          break;
        } catch (InterruptedException e) {
          // swallow
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  // Block until all queued tasks are run.
  // Re-throws the most recently encountered exception, if any.
  public void flush() throws AsyncTaskException, InterruptedException {
    assertNotClosed();
    try {
      lock.writeLock().lock();
      for (BlockingQueue<AsyncTask> queue : queues.values()) {
        while (!queue.isEmpty()) {
          try {
            queue.wait();
          } catch (InterruptedException e) {
            // swallow?
          }
        }
      }
      if (latestException != null) {
        Throwable inner = latestException;
        latestException = null;
        throw new AsyncTaskException(inner);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private synchronized BlockingQueue<AsyncTask> getQueue(int partition) {
    return queues.computeIfAbsent(partition, (k) -> makeQueue());
  }

  private BlockingQueue<AsyncTask> makeQueue() {
    BlockingQueue<AsyncTask> queue = new ArrayBlockingQueue<>(queueCapacity);
    Runnable main =
        () -> {
          while (true) {
            AsyncTask task;
            try {
              task = queue.take();
            } catch (InterruptedException e) {
              continue;
            }
            if (task == eof) {
              // exit thread when oef marker is reached
              return;
            }
            try {
              task.run();
              queue.notifyAll(); // wakeup wait() in flush()
            } catch (Exception e) {
              latestException = e;
            }
          }
        };
    Thread thread = new Thread(main);
    thread.start();
    return queue;
  }

  public synchronized void shutdown() throws AsyncTaskException {
    assertNotClosed();
    for (BlockingQueue<AsyncTask> queue : queues.values()) {
      // send eof marker to trigger shutdown
      try {
        queue.put(eof);
      } catch (InterruptedException e) {
        // swallow?
      }
    }
    try {
      flush();
    } catch (InterruptedException e) {
      // swallow
    }
    closed = true;
  }

  private void assertNotClosed() {
    if (closed) {
      throw new IllegalStateException(getClass().getName() + " was shutdown already.");
    }
  }

  @Override
  protected void finalize() {
    if (!closed) {
      try {
        shutdown();
      } catch (AsyncTaskException e) {
        // swallow
      }
    }
  }
}
