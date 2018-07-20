package io.skytty.karass;

public interface Sink<T> {

  void send(String key, T value);

  default void shutdown() {}

  default void init() {}
}
