package io.skytty.karass;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class SourceSinkSpec {

  @Test
  public void testSourceSink() throws UnsupportedEncodingException {
    String input =
        "      strlen is the thing with pointers -  \n"
            + "that iterates the chars -            \n"
            + "and counts the spots before the null \n"
            + "or never stops at all";

    String expected =
        "strlen is the thing with pointers -"
            + "that iterates the chars -"
            + "and counts the spots before the null"
            + "or never stops at all";

    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    Source<String> source = new Sources.IOSource(inputStream);
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    Sink<String> sink = new Sinks.IOSink(buf);
    source.fmap(x -> x.trim()).drainTo(sink);
    assertThat(buf.toString("UTF-8"), is(expected));
  }
}
