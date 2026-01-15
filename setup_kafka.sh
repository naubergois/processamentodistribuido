#!/bin/bash

# Configuration
KAFKA_VERSION="3.6.1"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_TGZ="${KAFKA_DIR}.tgz"
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

echo "Checking for Kafka..."

if [ ! -d "$KAFKA_DIR" ]; then
    if [ ! -f "$KAFKA_TGZ" ]; then
        echo "Downloading Kafka ${KAFKA_VERSION}..."
        curl -O $DOWNLOAD_URL
    fi
    echo "Extracting..."
    tar xf $KAFKA_TGZ
else
    echo "Kafka directory exists."
fi

# Set JAVA_HOME if helpful (brew install openjdk location)
if [ -d "/opt/homebrew/opt/openjdk/bin" ]; then
    export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
    echo "Added Brew OpenJDK to PATH"
fi

echo "Setup Complete. To start Kafka:"
echo "1. Start Zookeeper:  ./$KAFKA_DIR/bin/zookeeper-server-start.sh config/zookeeper.properties"
echo "2. Start Kafka:      ./$KAFKA_DIR/bin/kafka-server-start.sh config/server.properties"
