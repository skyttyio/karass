package io.skytty.karass.http;

import io.skytty.karass.Bus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class BusServlet extends HttpServlet {

  static final int DEFAULT_MAX_EVENTS_PER_REQUEST = 100;

  protected int maxEventsPerRequest = DEFAULT_MAX_EVENTS_PER_REQUEST;

  protected abstract Bus<String> getBus(String path);

  private static void require(String name, Object obj) throws ServletException {
    if (obj == null) {
      throw new ServletException("missing parameter:" + name);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
    String path = req.getPathTranslated();
    String sinceParam = req.getParameter("i");
    String maxEventsParam = req.getParameter("n");
    int since = Optional.ofNullable(sinceParam).map(Integer::parseInt).orElse(0);
    int maxEvents =
        Optional.ofNullable(maxEventsParam).map(Integer::parseInt).orElse(maxEventsPerRequest);

    if (maxEvents > maxEventsPerRequest) {
      maxEvents = maxEventsPerRequest;
    }
    try {
      getBus(path).process(e -> println(resp, e.value), since, maxEvents);
    } catch (Exception e) {
      throw new ServletException("could not write events", e);
    }
  }

  private void println(HttpServletResponse resp, String s) {
    try {
      resp.getWriter().println(s);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
    String path = req.getPathTranslated();
    String k = req.getParameter("k");
    String v = req.getParameter("v");
    require(k, "k");
    require(v, "v");
    getBus(path).emit(k, v);
  }
}
