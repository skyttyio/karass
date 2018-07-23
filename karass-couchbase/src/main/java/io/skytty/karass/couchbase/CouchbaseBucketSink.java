package io.skytty.karass.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonStringDocument;
import io.skytty.karass.Sink;
import io.skytty.karass.util.AsyncTaskRunner;
import io.skytty.karass.util.AsyncTaskRunner.AsyncTaskException;
import java.io.IOException;
import java.io.UncheckedIOException;

public class CouchbaseBucketSink extends Sink<String> {

  protected AsyncTaskRunner runner;
  protected Bucket bucket;

  public CouchbaseBucketSink(Bucket bucket) {
    this(bucket, new AsyncTaskRunner());
  }

  public CouchbaseBucketSink(Bucket bucket, AsyncTaskRunner runner) {
    this.bucket = bucket;
    this.runner = runner;
  }

  // override to customize upsert operation
  protected void upsert(String key, String value) {
    bucket.upsert(JsonStringDocument.create(key, value));
  }

  @Override
  public void send(String key, String value) throws IOException {
    runner.submit(key, () -> upsert(key, value));
  }

  @Override
  public void shutdown() {
    try {
      runner.shutdown();
    } catch (AsyncTaskException e) {
      throw new UncheckedIOException(e);
    }
  }
}
