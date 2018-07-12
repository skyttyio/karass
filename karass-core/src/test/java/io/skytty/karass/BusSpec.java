package io.skytty.karass;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class BusSpec {

  class Counter {
    int count = 0;

    public synchronized void inc() {
      count++;
    }

    public int get() {
      return count;
    }
  }

  @Test
  public void availableEvents() {
    Bus<String> b = new Bus();
    b.emit("key1", "ryanne");
    b.emit("key2", "sarah");
    final Counter count = new Counter();
    b.process(e -> count.inc(), 0);
    assertThat(count.get(), is(2));
  }

  @Test
  public void logCompaction() {
    Bus<String> b = new Bus();
    b.emit("key", "ryanne");
    b.emit("key", "sarah");
    final Counter count = new Counter();
    b.process(e -> count.inc(), 0);
    assertThat(count.get(), is(1));
  }

  @Test
  public void getKey() {
    Bus<String> b = new Bus();
    b.emit("key", "foo");
    assertThat(b.get("key"), is("foo"));
  }

  @Test
  public void boundedBus() {
    int capacity = 3;
    Bus<Integer> b = new BoundedBus(capacity);
    for (int i = 0; i < 100; i++) {
      String s = Integer.toString(i);
      b.emit(s, i);
    }
    final Counter count = new Counter();
    b.process(e -> count.inc(), 0);
    assertThat(count.get(), is(capacity));
  }
}
