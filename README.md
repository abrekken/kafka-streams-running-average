## Running Average Stream Processor

This is a simple Kafka Steams application to compute a running average for a 5-minute windowed 
aggregation. The version of the Kafka Streams library being used supports Mac silicon. (necessary for the embedded
RocksDB database used in the windowed aggregation)  