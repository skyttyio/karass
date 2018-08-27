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
    Bus<String> b = new Bus<>();
    b.emit("key1", "ryanne");
    b.emit("key2", "sarah");
    final Counter count = new Counter();
    b.process(e -> count.inc(), 0);
    assertThat(count.get(), is(2));
  }

  @Test
  public void logCompaction() {
    Bus<String> b = new Bus<>();
    b.emit("key", "ryanne");
    b.emit("key", "sarah");
    final Counter count = new Counter();
    b.process(e -> count.inc(), 0);
    assertThat(count.get(), is(1));
  }

  @Test
  public void getKey() {
    Bus<String> b = new Bus<>();
    b.emit("key", "foo");
    assertThat(b.get("key"), is("foo"));
  }

  @Test
  public void boundedBus() {
    int capacity = 3;
    Bus<Integer> b = new BoundedBus<>(capacity);
    for (int i = 0; i < 100; i++) {
      String s = Integer.toString(i);
      b.emit(s, i);
    }
    final Counter count = new Counter();
    b.process(e -> count.inc(), 0);
    assertThat(count.get(), is(capacity));
  }

  @Test(timeout = 1000)
  public void complexFlow() {
    Bus<Integer> a = new Bus<>();
    Bus<String> b = a.filter(i -> i % 10 == 0).fmap(i -> Integer.toString(i));
    for (int i = 0; i < 100; i++) {
      String s = Integer.toString(i);
      a.emit(s, i);
    }
    a.close();
    final Counter count1 = new Counter();
    a.foreach(e -> count1.inc());
    assertThat(count1.get(), is(100));
    final Counter count2 = new Counter();
    b.foreach(e -> count2.inc());
    assertThat(count2.get(), is(10));
  }

  @Test
  public void joinReduce() {
    Bus<Integer> a = new Bus<>();
    Bus<Integer> b = new Bus<>();
    for (int i = 0; i < 10; i++) {
      String s = Integer.toString(i);
      a.emit(s, i);
      b.emit(s, i * 2);
    }
    a.close();
    b.close();
    Bus<Integer> c = a.join(b).fmap(x -> x.left + x.right);
    int sum = c.reduce(0, (agg, x) -> agg + x);
    assertThat(sum, is(135));
  }

  private static <T> T ident(T t) {
    return t;
  }

  @Test
  public void variance() {
    Bus<Integer> a = new Bus<>();
    a.close();
    Bus<Number> b = a.fmap(BusSpec::ident);
    b.foreach(System.out::println);
  }
}
