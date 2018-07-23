package io.skytty.karass.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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

  static final int DEFAULT_NUM_PARTITIONS = 4;
  static final int DEFAULT_QUEUE_CAPACITY = 16;
  static final AsyncTask eof = () -> {};

  private Map<Integer, BlockingQueue<AsyncTask>> queues = new HashMap<>();
  protected int numPartitions = DEFAULT_NUM_PARTITIONS;
  protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
  private Throwable latestException = null;
  private boolean closed = false;

  public AsyncTaskRunner(int numPartitions, int queueCapacity) {
    this.numPartitions = numPartitions;
    this.queueCapacity = queueCapacity;
  }

  public AsyncTaskRunner() {}

  public void submit(String key, AsyncTask task) {
    submit(key.hashCode() % numPartitions, task);
  }

  public synchronized void submit(int partition, AsyncTask task) {
    assertNotClosed();
    BlockingQueue<AsyncTask> queue = queues.computeIfAbsent(partition, (k) -> makeQueue());
    while (true) {
      try {
        queue.put(task);
        break;
      } catch (InterruptedException e) {
        // swallow
      }
    }
    queue.notify();
  }

  // Block until all queued tasks are run.
  // Re-throws the most recently encountered exception, if any.
  public synchronized void flush() throws AsyncTaskException, InterruptedException {
    assertNotClosed();
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
