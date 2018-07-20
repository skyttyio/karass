package io.skytty.karass.azure;

import io.skytty.karass.Sink;
import io.skytty.karass.util.RetryPolicy;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

public class AzureEventHubSink extends Sink<ByteBuffer> {

  protected EventHubClient client;
  boolean shouldClose = false;
  RetryPolicy retryPolicy = new RetryPolicy();

  public AzureEventHubSink(EventHubClient client) {
    this.client = client;
  }

  public AzureEventHubSink(String connectionString) {
    client = EventHubClient.createSync(connectionString);
    shouldClose = true;
  }

  @Override
  public void shutdown() {
    if (shouldClose) {
      client.closeSync();
    }
  }

  @Override
  public void send(String key, ByteBuffer value) throws IOException {
    Runnable runnable = () => {
      EventData data = EventData.create(value);
      client.sendSync(data);
    };
    retryPolicy.retry(runnable);
  }

  public void setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }
}
