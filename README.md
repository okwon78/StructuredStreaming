# Structured Streaming Quick Example

## 1. [Structured Streaming ETL](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/StructuredStreamingETL.scala)

## 2. [Streaming Aggregations](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/StreamingAggregations.scala)

## 3. [Thumbling Window](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/ThumblingWindow.scala)

## 4. [Sliding Window](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/SlidingWindow.scala)

## 5. [Streaming Triggers](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/StreamingTriggers.scala)

## 6. [Elasticsearch Sink](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/ElasticsearchSink.scala)

## 7. [NetworkWordCount](https://github.com/okwon78/StructuredStreaming/blob/master/src/main/scala/NetworkWordCount.scala)
Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP socket. Let’s see how you can express this using Structured Streaming.

# Structured Streaming Build and Test

## Compile 

You will first need to compile scala code with sbt

```bash
sbt assembly
```

## network Input
You need to run Netcat as a data server

```bash
nc -lk 999
```

### Run Structured Streaming

you can start the example by using run.sh

```bash
run.sh
```



