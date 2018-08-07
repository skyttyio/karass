package io.skytty.karass;

import java.util.function.Consumer;

// a Source with a backing key-value Store
public class StoredSource<T> extends EventStream<T> implements Store<T> {

  protected Store<T> store;
  protected Source<T> source;

  public StoredSource(Store<T> store, Source<T> source) {
    this.store = store;
    this.source = source;
  }

  @Override
  public T get(String key) {
    return store.get(key);
  }

  @Override
  protected void foreachEvent(Consumer<Event<T>> f) {
    source.foreachEvent(f);
  }
}
