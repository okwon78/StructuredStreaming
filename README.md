# Structured Streaming Quick Example

## 1. Structured Streaming ETL

## 2. Thumbling Window

## 3. Streaming Triggers

## 4. Streaming Aggregations

## 5. Sliding Window

## 6. Elasticsearch Sink

## 7. NetworkWordCount
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



