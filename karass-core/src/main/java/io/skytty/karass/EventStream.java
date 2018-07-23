package io.skytty.karass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class EventStream<T> {

  protected static class Event<T> {
    public String key;
    public T value;

    public Event(String k, T v) {
      this.key = k;
      this.value = v;
    }
  }

  protected abstract void foreachEvent(Consumer<Event<T>> f);

  public <U> Bus<U> fmap(Function<T, U> f) {
    Bus<U> child = new Bus<>();
    foreachEventAsync(e -> child.emit(e.key, f.apply(e.value))).thenRun(() -> child.close());
    return child;
  }

  public Bus<T> filter(Function<T, Boolean> f) {
    Bus<T> child = new Bus<>();
    foreachEventAsync(
            e -> {
              if (f.apply(e.value)) {
                child.emit(e.key, e.value);
              }
            })
        .thenRun(() -> child.close());
    return child;
  }

  public void foreach(Consumer<T> f) {
    foreachEvent(e -> f.accept(e.value));
  }

  protected CompletableFuture<Void> foreachEventAsync(Consumer<Event<T>> f) {
    return CompletableFuture.runAsync(() -> foreachEvent(f));
  }

  public CompletableFuture<Void> foreachAsync(Consumer<T> f) {
    return CompletableFuture.runAsync(() -> foreach(f));
  }

  public Bus<T> mapKeys(Function<String, String> f) {
    Bus<T> child = new Bus<>();
    foreachEventAsync(e -> child.emit(f.apply(e.key), e.value)).thenRun(() -> child.close());
    return child;
  }

  public Bus<T> filterKeys(Function<String, Boolean> f) {
    Bus<T> child = new Bus<>();
    foreachEventAsync(
            e -> {
              if (f.apply(e.key)) {
                child.emit(e.key, e.value);
              }
            })
        .thenRun(() -> child.close());
    return child;
  }

  public void drainTo(Sink<T> sink) throws IOException {
    foreachEvent(
        x -> {
          try {
            sink.send(x.key, x.value);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }
}
