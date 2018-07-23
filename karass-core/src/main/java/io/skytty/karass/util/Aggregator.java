package io.skytty.karass.util;

import java.util.function.Function;

public class Aggregator<T> {

  T t;

  public Aggregator(T t) {
    this.t = t;
  }

  public void apply(Function<T, T> f) {
    t = f.apply(t);
  }

  public T get() {
    return t;
  }
}
