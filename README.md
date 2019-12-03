# Structured Streaming Quick Example

## Structured Streaming ETL

## Thumbling Window

## Streaming Triggers

## Streaming Aggregations

## Sliding Window

## Elasticsearch Sink

## NetworkWordCount
Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP socket. Let’s see how you can express this using Structured Streaming.


### Compile 

You will first need to compile scala code with sbt

```bash
sbt assembly
```

### network Input
You need to run Netcat as a data server

```bash
nc -lk 999

```

### Run Structured Streaming

you can start the example by using run.sh

```bash
run.sh
```



