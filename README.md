# karass
Stream processing library for Java

## Motivation

Karass aims to provide a single functional API for both tables and streams.

## Examples

    Source<String> source = new AzureEventHubSource(...);
    Sink<String> sink = new AzureCosmosTableSink(...);

    source.fmap(x -> x.trim()).filter(x -> !x.isEmpty()).drain(sink);


