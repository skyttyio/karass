package io.skytty.karass;

import java.io.IOException;

public abstract class Sink<T> {

  public abstract void send(String key, T value) throws IOException;

  public void shutdown() {}

  public void init() {}
}
