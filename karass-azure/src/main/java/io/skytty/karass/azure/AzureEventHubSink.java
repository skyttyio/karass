package io.skytty.karass.azure;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import io.skytty.karass.Sink;
import io.skytty.karass.util.RetryPolicy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

public class AzureEventHubSink extends Sink<ByteBuffer> {

  protected EventHubClient client;
  boolean shouldClose = false;
  RetryPolicy retryPolicy = new RetryPolicy();

  public AzureEventHubSink(EventHubClient client) {
    this.client = client;
  }

  public AzureEventHubSink(String connectionString, ExecutorService executor)
      throws EventHubException, IOException {
    client = EventHubClient.createSync(connectionString, executor);
    shouldClose = true;
  }

  @Override
  public void shutdown() {
    if (shouldClose) {
      try {
        client.closeSync();
      } catch (EventHubException e) {
        // swallow
      }
    }
  }

  @Override
  public void send(String key, ByteBuffer value) throws IOException {
    retryPolicy.retry(
        () -> {
          EventData data = EventData.create(value);
          client.sendSync(data);
        });
  }

  public void setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }
}
