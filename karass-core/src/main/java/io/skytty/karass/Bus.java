package io.skytty.karass;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/** Log-compacted event bus. */
public class Bus<T> {

  public static class Event<T> {
    public String key;
    public T value;

    public Event(String k, T v) {
      this.key = k;
      this.value = v;
    }
  }

  int count = 0;
  boolean closed = false;
  Map<Integer, Event<T>> events = new HashMap();
  Map<String, Integer> offsets = new HashMap();

  public synchronized int emit(String key, T value) {
    Event<T> event = new Event(key, value);
    if (closed) {
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
    closed = true;
  }

  // apply f to all events since given offset. Will block until new events arrive.
  public int process(Consumer<Event<T>> f, int since) {
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

  public void waitForOffset(int offset) {
    while (count < offset && !closed) {
      try {
        synchronized (this) {
          wait();
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  // create a thread to process every event
  public Thread thread(Consumer<Event<T>> f) {
    return new Thread(runnable(f));
  }

  // create a runnable to process every event
  public Runnable runnable(Consumer<Event<T>> f) {
    return new Runnable() {
      @Override
      public void run() {
        int i = 0;
        while (!closed) {
          i = process(f, i);
        }
      }
    };
  }

  public <U> Bus<U> fmap(Function<T, U> f) {
    Bus<U> child = new Bus();
    thread(e -> child.emit(e.key, f.apply(e.value))).start();
    return child;
  }

  public Bus<T> filter(Function<T, Boolean> f) {
    Bus<T> child = new Bus();
    thread(
            e -> {
              if (f.apply(e.value)) {
                child.emit(e.key, e.value);
              }
            })
        .start();
    return child;
  }

  public void foreach(Consumer<T> f) {
    int i = 0;
    while (!closed) {
      i = process(e -> f.accept(e.value), i);
    }
  }
}