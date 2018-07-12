package io.skytty.karass;

public class BoundedBus<T> extends Bus<T> {

  int capacity;
  int oldest = 0;

  public BoundedBus(int capacity) {
    super();
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0");
    }
    this.capacity = capacity;
  }

  @Override
  public synchronized int emit(String key, T value) {
    int newest = super.emit(key, value);
    while (events.size() > capacity && oldest < newest) {
      forgetOffset(oldest);
      oldest++;
    }
    return newest;
  }
}
