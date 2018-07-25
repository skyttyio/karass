# karass
Stream processing library for Java

## Examples

    Source<String> source = new AzureEventHubSource(...);
    Sink<String> sink = new AzureCosmosTableSink(...);

    source.fmap(x -> x.trim()).filter(x -> !x.isEmpty()).drainTo(sink);


