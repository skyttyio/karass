package io.skytty.karass.http;

import io.skytty.karass.Bus;

public class SingleBusServlet extends BusServlet {

  private Bus<String> bus = new Bus<>();

  @Override
  protected Bus<String> getBus(String path) {
    return bus;
  }
}
