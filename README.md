# Apache Kafka Committer

Apache Kafka implementation of Norconex Committer.

# Configuration

When used with a Norconex Collector, you can use the following XML to configure Apache Kafka as the <committer> section of your Norconex Collector configuration:
  
    <committer class="net.danizen.norconex.committer.kafka.KafkaCommitter">
      <brokerList>...</brokerList>
      <topicName>...</topicName>

      <sourceReferenceField keep="[false|true]">...</sourceReferenceField>
      <sourceContentField keep="[false|true]">...</sourceContentField>
      <targetContentField>...</targetContentField>
      <queueDir>...</queueDir>
      <queueSize>...</queueSize>
      <commitBatchSize>...</commitBatchSize>
      <maxRetries>...</maxRetries>
      <maxRetryWait>...</maxRetryWait>
    </committer>
    
| Tag           | Description   |
| ------------- |:-------------:|
| brokerList    | Comma delimited list of host URLs to connect to a Kafka Broker of Cluster |
| topicName     | Kafka Topic to where the committer publish messages   |
