package io.skytty.karass;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/** Log-compacted event bus. */
public class Bus<T> {

  protected static class Event<T> {
    public String key;
    public T value;

    public Event(String k, T v) {
      this.key = k;
      this.value = v;
    }
  }

  int count = 0; // might need atomic int
  int closedAt = -1;
  Map<Integer, Event<T>> events = new HashMap<>();
  Map<String, Integer> offsets = new HashMap<>();

  public synchronized int emit(String key, T value) {
    Event<T> event = new Event(key, value);
    if (closed()) {
      throw new IllegalStateException("Bus is closed.");
    }
    int offset = count;
    count++;
    events.put(offset, event);
    Integer prev = offsets.get(event.key);
    if (prev != null) {
      events.remove(prev);
    }
    offsets.put(event.key, offset);
    notifyAll();
    return offset;
  }

  protected synchronized void forgetOffset(int offset) {
    Event<T> existing = events.get(offset);
    if (existing != null && offsets.get(existing.key) == offset) {
      events.remove(offset);
      offsets.remove(existing.key);
    }
  }

  public synchronized T get(String key) {
    Integer i = offsets.get(key);
    if (i == null) {
      return null;
    }
    return events.get(i).value;
  }

  public synchronized void close() {
    closedAt = count;
    notifyAll();
  }

  public boolean closed() {
    return closedAt != -1;
  }

  // apply f to all events since given offset. Will block until new events arrive.
  protected int process(Consumer<Event<T>> f, int since) {
    waitForOffset(since);
    int i = since;
    for (; i < count; i++) {
      Event<T> e = events.get(i);
      if (e != null) {
        f.accept(e);
      }
    }
    return i;
  }

  protected boolean mightReachOffset(int offset) {
    return closedAt == -1 || closedAt > offset;
  }

  protected boolean reachedOffset(int offset) {
    return count - 1 >= offset;
  }

  protected void waitForOffset(int offset) {
    while (!reachedOffset(offset) && mightReachOffset(offset)) {
      try {
        synchronized (this) {
          System.out.println(this.toString() + " waiting for " + Integer.toString(offset) + "...");
          wait();
          System.out.println(this.toString() + "woken up at " + Integer.toString(count) + "...");
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  public <U> Bus<U> fmap(Function<T, U> f) {
    Bus<U> child = new Bus();
    foreachEventAsync(e -> child.emit(e.key, f.apply(e.value))).thenRun(() -> child.close());
    return child;
  }

  public Bus<T> filter(Function<T, Boolean> f) {
    Bus<T> child = new Bus();
    foreachEventAsync(
            e -> {
              if (f.apply(e.value)) {
                child.emit(e.key, e.value);
              }
            })
        .thenRun(() -> child.close());
    return child;
  }

  protected void foreachEvent(Consumer<Event<T>> f) {
    int i = 0;
    while (mightReachOffset(i)) {
      i = process(e -> f.accept(e), i);
    }
  }

  public void foreach(Consumer<T> f) {
    foreachEvent(e -> f.accept(e.value));
  }

  protected CompletableFuture<Void> foreachEventAsync(Consumer<Event<T>> f) {
    return CompletableFuture.runAsync(() -> foreachEvent(f));
  }

  public CompletableFuture<Void> foreachAsync(Consumer<T> f) {
    return CompletableFuture.runAsync(() -> foreach(f));
  }

  public Bus<T> mapKeys(Function<String, String> f) {
    Bus<T> child = new Bus();
    foreachEventAsync(e -> child.emit(f.apply(e.key), e.value)).thenRun(() -> child.close());
    return child;
  }

  public Bus<T> filterKeys(Function<String, Boolean> f) {
    Bus<T> child = new Bus();
    foreachEventAsync(
            e -> {
              if (f.apply(e.key)) {
                child.emit(e.key, e.value);
              }
            })
        .thenRun(() -> child.close());
    return child;
  }
}
