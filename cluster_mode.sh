#!/bin/bash


spark-submit --master yarn  \
            --deploy-mode cluster \
            --class KafkaSource \
            --driver-memory 2g \
            StructuredStreaming-assembly-0.2.jar
