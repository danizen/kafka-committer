/* Copyright 2017 Daniel Davis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.danizen.norconex.committer.kafka;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * Commits documents to Kafka using Producer API
 * </p>
 * <p>
 * Despite being a subclass
 * of {@link AbstractMappedCommitter}, setting an <code>idTargetField</code>
 * is not supported.
 * </p>
 * <p>
 * As of 3.0.0, XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per
 * {@link } (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p>
 * <h3>XML configuration usage:</h3>
 *
 * <pre>
 *  &lt;committer class="com.norconex.committer.elasticsearch.ElasticsearchCommitter"&gt;
 *      &lt;indexName&gt;(Name of the index to use)&lt;/indexName&gt;
 *      &lt;typeName&gt;(Name of the type to use)&lt;/typeName&gt;
 *      &lt;clusterName&gt;
 *         (Name of the ES cluster to join. Default is "elasticsearch".)
 *      &lt;/clusterName&gt;
 *      &lt;clusterHosts&gt;
 *      	(Comma delimited list of hosts to connect to join the cluster.
 *      	Default is "localhost".)
 *      &lt;/clusterHosts&gt;
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when
 *         the default document reference is not used.  The reference value
 *         will be mapped to the Elasticsearch ID field.
 *         Once re-mapped, this metadata source field is
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document
 *         "content", you can specify that field here.  Default
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is "content".)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send to Elasticsearch at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay in milliseconds between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 *
 * @author Pascal Dimassimo
 * @author Pascal Essiembre
 */
public class KafkaCommitter extends AbstractMappedCommitter {

    private static final Logger logger = LogManager
            .getLogger(KafkaCommitter.class);

    private static final String DFLT_KAFKA_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";

    private String topicName;
    private String brokerList;
    private KafkaProducer<String, String> producer;

    private String jsonFieldsPattern;
    private String dotReplacement;

    /**
     * Constructor.
     */
    public KafkaCommitter() {
        super();
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    /**
     * Gets the regular expression matching fields that contains a JSON
     * object for its value (as opposed to a regular string).
     * Default is <code>null</code>.
     *
     * @return regular expression
     * @since 4.1.0
     */
    public String getJsonFieldsPattern() {
        return jsonFieldsPattern;
    }

    /**
     * Sets the regular expression matching fields that contains a JSON
     * object for its value (as opposed to a regular string).
     *
     * @param jsonFieldsPattern regular expression
     * @since 4.1.0
     */
    public void setJsonFieldsPattern(String jsonFieldsPattern) {
        this.jsonFieldsPattern = jsonFieldsPattern;
    }

    /**
     * Gets the character used to replace dots in field names.
     * Default is <code>null</code> (does not replace dots).
     *
     * @return replacement character or <code>null</code>
     */
    public String getDotReplacement() {
        return dotReplacement;
    }

    /**
     * Sets the character used to replace dots in field names.
     *
     * @param dotReplacement replacement character or <code>null</code>
     */
    public void setDotReplacement(String dotReplacement) {
        this.dotReplacement = dotReplacement;
    }

    /**
     * Responsible to create the producer based
     * on the parameters.
     */
    public synchronized KafkaProducer<String, String> createProducer() {
        if (producer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", getBrokerList());
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", getCommitBatchSize());
            props.put("linger.ms", 250);
            props.put("buffer.memory", 1 * 1024 * 1024);
            props.put("key.serializer", DFLT_KAFKA_SERIALIZER);
            props.put("value.serializer", DFLT_KAFKA_SERIALIZER);
            producer = new KafkaProducer<String, String>(props);
        }

        logger.info("Creating Kafka producer client: " + producer.metrics());
        return producer;
    }

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {

        logger.info("Processing batch to Kafka");

        if (producer == null) {
            createProducer();
        }

        for (ICommitOperation baseop : batch) {
            StringBuilder json = new StringBuilder();
            // NOTE: Probably need a marshalling class here that is pluggable
            if (baseop instanceof IAddOperation) {
                IAddOperation op = (IAddOperation) baseop;
                appendAddOperation(json, op);

                ProducerRecord<String, String> oprec = new ProducerRecord<String, String>
                        (getTopicName(), op.getReference(), json.toString());

                logger.info("Pushing message to Kafka");
                producer.send(oprec);
            } else if (baseop instanceof IDeleteOperation) {
                IDeleteOperation op = (IDeleteOperation) baseop;

                appendDeleteOperation(json, op);

                ProducerRecord<String, String> oprec = new ProducerRecord<String, String>
                        (getTopicName(), op.getReference(), json.toString());

                logger.info("Pushing message to Kafka");
                producer.send(oprec);
            }
        }
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {

        if (StringUtils.isNotBlank(topicName)) {
            writer.writeStartElement("topicName");
            writer.writeCharacters(topicName);
            writer.writeEndElement();
        }

        if (StringUtils.isNotBlank(brokerList)) {
            writer.writeStartElement("brokerList");
            writer.writeCharacters(brokerList);
            writer.writeEndElement();
        }
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setTopicName(xml.getString("topicName", null));
        setBrokerList(xml.getString("brokerList", null));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().appendSuper(super.hashCode())
                .append(topicName)
                .append(brokerList)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KafkaCommitter)) {
            return false;
        }
        KafkaCommitter other = (KafkaCommitter) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(topicName, other.topicName)
                .append(brokerList, other.brokerList)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("topicName", topicName)
                .append("brokerList", brokerList)
                .toString();
    }

    private void appendAddOperation(StringBuilder json, IAddOperation add) {
        String id = add.getMetadata().getString(getSourceReferenceField());
        if (StringUtils.isBlank(id)) {
            id = add.getReference();
        }
        json.append("{");
        append(json, "id", id);
        json.append(",");
        boolean first = true;
        for (Map.Entry<String, List<String>> entry : add.getMetadata().entrySet()) {
            String field = entry.getKey();
            field = StringUtils.replace(field, ".", dotReplacement);
            // Remove id from source unless specified to keep it
            if (!isKeepSourceReferenceField()
                    && field.equals(getSourceReferenceField())) {
                continue;
            }
            if (!first) {
                json.append(',');
            }
            append(json, field, entry.getValue());
            first = false;
        }
        json.append("}\n");
    }

    private void appendDeleteOperation(StringBuilder json, IDeleteOperation del) {
        json.append("{\"delete\":{");
        append(json, "id", del.getReference());
        json.append("}}\n");
    }

    private void append(StringBuilder json, String field, List<String> values) {
        if (values.size() == 1) {
            append(json, field, values.get(0));
            return;
        }
        json.append('"')
                .append(StringEscapeUtils.escapeJson(field))
                .append("\":[");
        boolean first = true;
        for (String value : values) {
            if (!first) {
                json.append(',');
            }
            appendValue(json, field, value);
            first = false;
        }
        json.append(']');
    }

    private void append(StringBuilder json, String field, String value) {
        json.append('"')
                .append(StringEscapeUtils.escapeJson(field))
                .append("\":");
        appendValue(json, field, value);
    }

    private void appendValue(StringBuilder json, String field, String value) {
        if (getJsonFieldsPattern() != null
                && getJsonFieldsPattern().matches(field)) {
            json.append(value);
        } else {
            json.append('"')
                    .append(StringEscapeUtils.escapeJson(value))
                    .append("\"");
        }
    }
}
