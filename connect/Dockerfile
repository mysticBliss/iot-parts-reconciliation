FROM confluentinc/cp-kafka-connect:7.6.0

# Install required Kafka Connect plugins from Confluent Hub
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-mqtt:1.7.1 \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
