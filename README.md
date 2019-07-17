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
    
Tag Descriptions:    
    
| Tag           | Description   |
| ------------- |:-------------:|
| brokerList    | Comma delimited list of host URLs to connect to a Kafka Broker of Cluster |
| topicName     | Kafka Topic to where the committer publish messages   |


# Installation

The Apache Kafka Committer is a library that you must include in another product classpath (along with required dependencies). For use with a Norconex Collector, the collector must already be installed on your system and is referred to as <collector_install_folder> in the following instructions. You have the option to perform an automated installation (recommended), or a manual one. 

## Automated Install

   * Download the latest release of this Committer.
   * Unzip the downloaded file in a new location.
   * Execute the install.bat or install.sh based on your operating system.

You will be prompted to specify the full path to <collector_install_folder>/lib and libraries will be copied for you. If you choose to rename conflicting files, you can safely delete any created backups once you confirmed your setup is working as it should. 

## Manual Install

   * Download the latest release of this Committer.
   * Unzip the downloaded file in a new location.
   * Copy the content of the <committer_unzipped_folder>/lib folder to <collector_install_folder>/lib.
   * If you notice different versions of the same library in the lib folder it is usually best advised to keep the greatest version only.
