#!/bin/sh

../../spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class WordCount --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0 target/scala-2.11/spark-streaming-with-kafka_2.11-1.0.jar -t wc1
