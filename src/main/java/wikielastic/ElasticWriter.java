package wikielastic;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.wiki.MergedWikiPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;


/**
 * Created by andrewcholakian on 6/5/15.
 */
public class ElasticWriter {
    public final static Logger logger = LoggerFactory.getLogger(ElasticWriter.class);
    final Client client;
    final String indexName;
    final String typeName = "page";
    final String pageMapping = "{\"page\": {\"properties\":{\"redirects\":{\"fields\":{\"redirects_snow\":{\"type\":\"string\",\"analyzer\":\"snowball\"},\"redirects_simple\":{\"type\":\"string\",\"analyzer\":\"simple\"},\"redirects_exact\":{\"index\":\"not_analyzed\",\"type\":\"string\"}},\"type\":\"string\"},\"format\":{\"index\":\"not_analyzed\",\"type\":\"string\"},\"ns\":{\"index\":\"not_analyzed\",\"type\":\"string\"},\"title\":{\"fields\":{\"title_snow\":{\"type\":\"string\",\"analyzer\":\"snowball\"},\"title_exact\":{\"index\":\"not_analyzed\",\"type\":\"string\"},\"title_simple\":{\"type\":\"string\",\"analyzer\":\"simple\"}},\"type\":\"string\"},\"suggest\":{\"index_analyzer\":\"simple\",\"type\":\"completion\",\"search_analyzer\":\"simple\"},\"redirect\":{\"index\":\"not_analyzed\",\"type\":\"string\"},\"timestamp\":{\"type\":\"date\"},\"body\":{\"fields\":{\"body_snow\":{\"type\":\"string\",\"analyzer\":\"snowball\"},\"body_simple\":{\"type\":\"string\",\"analyzer\":\"simple\"}},\"type\":\"string\"}},\"_all\":{\"_enabled\":false}}}";

    public ElasticWriter(String indexName) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "avc-cluster").build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
        this.indexName = indexName;
    }

    public void setupIndex() {
        if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Settings irSettings = ImmutableSettings.settingsBuilder().
                put("number_of_shards", 1).
                put("number_of_replicas", 0).
                put("refresh_interval", 3000).
                put("gateway.local.sync", "120s").
                put("store.throttle.max_bytes_per_sec", 200).
                put("store.throttle.type", "none").
                build();

        CreateIndexResponse resp = client.admin().indices().prepareCreate(indexName).
                setSettings(irSettings).
                addMapping(typeName, pageMapping).execute().actionGet();

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public BulkResponse write(List<MergedWikiPage> pages) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        pages.stream().forEach(page -> {
            List<String> suggest = new LinkedList();
            suggest.add(page.title);
            suggest.addAll(page.redirects);
            XContentBuilder x = null;
            try {
                x = jsonBuilder().
                        startObject().
                        field("title", page.title).
                        field("text", page.text).
                        field("body", page.redirects).
                        field("suggest", suggest).
                        field("timestamp", page.timestamp).
                        endObject();
            } catch (IOException e) {
                logger.error("Could not build JSON content for bulk request", e);
                System.exit(1);
            }

            bulkRequest.add(client.prepareIndex("en-wikipedia", "page").setSource(x));
        });

        return bulkRequest.execute().actionGet();
    }

    public void close() {
        client.close();
    }
}
