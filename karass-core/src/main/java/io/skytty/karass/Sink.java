package io.skytty.karass;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class Sink<T> {

  abstract void send(String key, T value) throws IOException;

  void shutdown() {}

  void init() {}

  void sendUnchecked(String key, T value) {
    try {
      send(key, value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
