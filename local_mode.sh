#!/bin/bash

spark-submit --master local[2] ./target/scala-2.11/structured-streaming-assembly-0.1.jar
