package io.skytty.karass.util;

import java.io.IOException;

public class RetryPolicy {

  static int DEFAULT_MAX_RETRIES = 3;
  static int DEFAULT_BACKOFF_MILLIS = 1000;

  public static class MaxRetriesException extends IOException {
    public MaxRetriesException(int retryCount, Exception inner) {
      super(String.format("gave up after %1$d tries", retryCount), inner);
    }
  }

  public interface RetryableTask {
    void run() throws Exception;
  }

  public class Retrying {

    private int retryCount = -1;
    private int delay = 0;
    private RetryableTask task;
    private boolean success = false;

    protected Retrying(RetryableTask task) {
      this.task = task;
    }

    public boolean shouldRetry() {
      return !success && retryCount < maxRetries;
    }

    private int nextDelay() {
      delay += backoffMillis;
      return delay;
    }

    public void tryRun() throws MaxRetriesException {
      try {
        int delay = nextDelay();
        if (delay > 0) {
          Thread.sleep(delay);
        }
        task.run();
        success = true;
      } catch (Exception e) {
        if (!shouldRetry()) {
          throw new MaxRetriesException(retryCount, e);
        }
      }
    }
  }

  protected int maxRetries = DEFAULT_MAX_RETRIES;
  protected int backoffMillis = DEFAULT_BACKOFF_MILLIS;

  public RetryPolicy(int maxRetries, int backoffMillis) {
    this.maxRetries = maxRetries;
    this.backoffMillis = backoffMillis;
  }

  public RetryPolicy() {}

  public void retry(RetryableTask task) throws MaxRetriesException {
    Retrying retrying = this.new Retrying(task);
    while (retrying.shouldRetry()) {
      retrying.tryRun();
    }
  }
}
