package io.skytty.karass;

public interface Store<T> {

  T get(String key);
}
