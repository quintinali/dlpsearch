package dlpsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * Driver implementation for all Elasticsearch functionality.
 */
public class ESDriver implements Serializable {

  private static final long serialVersionUID = 1L;
  private transient Client client = null;
  private transient Node node = null;
  private transient BulkProcessor bulkProcessor = null;

  /**
   * Substantiated constructor which accepts a {@link java.util.Properties}
   *
   */
  public ESDriver() {
    try {
      setClient(makeClient());
    } catch (IOException e) {
    }
  }

  public void createBulkProcessor() {
    setBulkProcessor(BulkProcessor.builder(getClient(), new BulkProcessor.Listener() {
      public void beforeBulk(long executionId, BulkRequest request) {
      }

      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
      }

      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        throw new RuntimeException("Caught exception in bulk: " + request.getDescription() + ", failure: " + failure, failure);
      }
    }).setBulkActions(1000).setBulkSize(new ByteSizeValue(2500500, ByteSizeUnit.GB)).setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 10)).setConcurrentRequests(1)
        .build());
  }

  public void destroyBulkProcessor() {
    try {
      getBulkProcessor().awaitClose(20, TimeUnit.MINUTES);
      setBulkProcessor(null);
      refreshIndex();
    } catch (InterruptedException e) {
    }
  }


  public void close() {
    client.close();
  }

  public void refreshIndex() {
    client.admin().indices().prepareRefresh().execute().actionGet();
  }

  /**
   * Generates a TransportClient or NodeClient
   *
   * @param props a populated {@link java.util.Properties} object
   * @return a constructed {@link org.elasticsearch.client.Client}
   * @throws IOException if there is an error building the
   *                     {@link org.elasticsearch.client.Client}
   */
  protected Client makeClient() throws IOException {
    String clusterName = SysConstants.ES_CLUSTER;
    String hostsString = SysConstants.ES_UNICAST_HOSTS;
    String[] hosts = hostsString.split(",");
    String portStr = SysConstants.ES_TRANSPORT_TCP_PORT;
    int port = Integer.parseInt(portStr);

    Settings.Builder settingsBuilder = Settings.builder();

    // Set the cluster name and build the settings
    if (!clusterName.isEmpty())
      settingsBuilder.put("cluster.name", clusterName);

    settingsBuilder.put("http.type", "netty3");
    settingsBuilder.put("transport.type", "netty3");

    Settings settings = settingsBuilder.build();

    Client client = null;

    // Prefer TransportClient
    if (hosts != null && port > 1) {
      TransportClient transportClient = new ESTransportClient(settings);
      for (String host : hosts)
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
      client = transportClient;
    } else if (clusterName != null) {
      node = new Node(settings);
      client = node.client();
    }

    return client;
  }

  /**
   * @return the client
   */
  public Client getClient() {
    return client;
  }

  /**
   * @param client the client to set
   */
  public void setClient(Client client) {
    this.client = client;
  }

  /**
   * @return the bulkProcessor
   */
  public BulkProcessor getBulkProcessor() {
    return bulkProcessor;
  }

  /**
   * @param bulkProcessor the bulkProcessor to set
   */
  public void setBulkProcessor(BulkProcessor bulkProcessor) {
    this.bulkProcessor = bulkProcessor;
  }

  
}
