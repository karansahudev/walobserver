#!/bin/bash

# Set environment variables for Kafka producer
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="your-kafka-topic"

# Make the variables available to all processes
echo "export KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS" >> ~/.bashrc
echo "export KAFKA_TOPIC=$KAFKA_TOPIC" >> ~/.bashrc

# Source the variables to the current session
source ~/.bashrc
