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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

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
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
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

    private String topicName;
    private String brokerList;

    /**
     * Constructor.
     */
    public KafkaCommitter() { }

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

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        throw new CommitterException("Not yet implemented");
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
}
