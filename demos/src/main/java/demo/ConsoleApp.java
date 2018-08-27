package demo;

import io.skytty.karass.Sinks;
import io.skytty.karass.Sources;
import java.io.IOException;

class ConsoleApp {

  public static void main(String args[]) throws IOException {
    System.out.println("hello!");
    Sources.STDIN.fmap(x -> "you said: " + x + "\n\n").drain(Sinks.STDOUT);
    System.out.println("done.");
  }
}
