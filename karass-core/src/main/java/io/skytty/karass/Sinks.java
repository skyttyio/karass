package io.skytty.karass;

import java.io.OutputStream;
import java.io.PrintStream;

public class Sinks {

  public static class Stdout extends IOSink {

    public Stdout() {
      super(System.out);
    }
  }

  public static class StdoutWithNewlines extends Stdout {

    @Override
    public void send(String key, String value) {
      super.send(key, value);
      out.println();
    }
  }

  public static class IOSink extends Sink<String> {

    PrintStream out;

    public IOSink(OutputStream out) {
      this.out = new PrintStream(out);
    }

    @Override
    public void send(String key, String value) {
      out.print(value);
    }
  }
}
