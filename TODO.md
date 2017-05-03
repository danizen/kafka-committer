- Probably need a topic for add operations and another for deletes,
  because the processing for each will be different in Spark.
- Probably need a KafkaProducerFactory interface and default implementation.
  This also isolates the stuff that relates to ZooKeeper and such.
- Probably need some abstraction to create ProducerEvent from IAddOperation
  or IDeleteOperation

