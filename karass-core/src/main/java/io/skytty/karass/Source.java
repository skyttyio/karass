package io.skytty.karass;

public abstract class Source<T> extends EventStream<T> {

  public void init() {}

  public void shutdown() {}
}
