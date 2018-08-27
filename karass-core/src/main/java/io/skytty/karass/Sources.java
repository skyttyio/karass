package io.skytty.karass;

import java.io.InputStream;
import java.util.Scanner;
import java.util.function.Consumer;

public class Sources {

  public static final Stdin STDIN = new Stdin();

  public static class Stdin extends IOSource {

    public Stdin() {
      super(System.in);
    }
  }

  public static class IOSource extends Source<String> {

    Scanner in;

    public IOSource(InputStream in) {
      this.in = new Scanner(in);
    }

    @Override
    public void shutdown() {}

    protected Event<String> readEvent() {
      String line = in.nextLine();
      return new Event<String>(line, line);
    }

    @Override
    protected void foreachEvent(Consumer<Event<String>> f) {
      while (in.hasNextLine()) {
        Event<String> event = readEvent();
        f.accept(event);
      }
    }
  }
}
