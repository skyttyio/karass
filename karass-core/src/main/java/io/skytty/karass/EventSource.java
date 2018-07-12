package io.skytty.karass;

public interface EventSource<T> {

  public void emit(String key, T event);
}
