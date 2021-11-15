/**
 * @author Alon Eirew
 */

package wiki.elastic;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import wiki.data.WikipediaParsedPage;
import wiki.data.obj.ReferenceContext;
import wiki.model.Cluster;
import wiki.model.IndexingConfiguration;
import wiki.utils.WikiToElasticConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

@Slf4j
public class IndexingAPI implements Closeable {

    private static final Gson GSON = new Gson();
    private final static int MAX_AVAILABLE = 5;

    private final AtomicInteger totalIdsProcessed = new AtomicInteger(0);
    private final AtomicInteger totalIdsSuccessfullyCommitted = new AtomicInteger(0);

    // Limit the number of threads accessing elastic in parallel
    private final Semaphore available = new Semaphore(MAX_AVAILABLE, true);
    private final RestHighLevelClient client;
    private final Object closeLock = new Object();

    private final String indexName;
    private final String docType;

    public IndexingAPI(IndexingConfiguration configuration) throws IOException {
        if (configuration.getIndexName() != null && !configuration.getIndexName().isEmpty() &&
                configuration.getDocType() != null && !configuration.getDocType().isEmpty()) {

            this.indexName = configuration.getIndexName();
            this.docType = configuration.getDocType();

        } else {
            throw new IOException("Missing mandatory values of \"indexName\" & \"docType\" in configuration");
        }

        // init elastic client
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(configuration.getHost(),
                                configuration.getPort(),
                                configuration.getScheme()))
                        //.setRequestConfigCallback(
                        //                requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(1000000)

    //            )
    );
    }

    public void deleteIndex() throws ConnectException {

        DeleteIndexResponse deleteIndexResponse;

        try {

            DeleteIndexRequest delRequest = new DeleteIndexRequest(this.indexName);
            this.available.acquire();
            deleteIndexResponse = this.client.indices().delete(delRequest);
            this.available.release();
            log.info("Index " + this.indexName + " deleted successfully: " + deleteIndexResponse.isAcknowledged());

        } catch (ElasticsearchException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.info("Index " + ese.getIndex().getName() + " not found");
            } else {
                log.debug(ese.toString());
            }
        } catch (ConnectException e) {
            log.error("Could not connect to elasticsearch...");
            throw e;
        } catch (IOException | InterruptedException e) {
            log.debug(e.toString());
        }

    }

    public void createIndex(IndexingConfiguration configuration) throws IOException {

        CreateIndexResponse createIndexResponse = null;

        try {
            // Create the index
            CreateIndexRequest crRequest = new CreateIndexRequest(configuration.getIndexName());

            // Create shards & replicas
            Settings.Builder builder = Settings.builder();
            builder
                    .put("index.number_of_shards", configuration.getShards())
                    .put("index.number_of_replicas", configuration.getReplicas());

            String settingFileContent = configuration.getSettingFileContent();

            if (settingFileContent != null && !settingFileContent.isEmpty()) {
                builder.loadFromSource(settingFileContent, XContentType.JSON);
            }

            crRequest.settings(builder);

            // Create index mapping
            String mappingFileContent = configuration.getMappingFileContent();

            if (mappingFileContent != null && !mappingFileContent.isEmpty()) {
                crRequest.mapping(configuration.getDocType(), mappingFileContent, XContentType.JSON);
            }

            this.available.acquire();
            createIndexResponse = this.client.indices().create(crRequest);
            this.available.release();

            log.info("Index " + configuration.getIndexName() + " created successfully: " + createIndexResponse.isAcknowledged());

        } catch (InterruptedException e) {
            log.error("Could not creat elasticsearch index");
        }

    }

    public synchronized void onSuccess(int successCount) {
        this.available.release();
        this.totalIdsSuccessfullyCommitted.addAndGet(successCount);
        this.totalIdsProcessed.addAndGet(-successCount);
        synchronized (closeLock) {
            closeLock.notify();
        }
    }

    // Those are requests that didnt change elastic (because nothing to update)
    public void updateNOOPs(int noops) {
        this.totalIdsSuccessfullyCommitted.addAndGet(-noops);
    }

    public synchronized void onFail(int failedCount) {
        this.available.release();
        this.totalIdsProcessed.addAndGet(-failedCount);
        synchronized (closeLock) {
            closeLock.notify();
        }
    }

    public IndexResponse addCluster(Cluster page) {

        IndexResponse res = null;

        try {
            if (isValidRequest(page)) {

                IndexRequest indexRequest = createIndexRequest(page);
                this.available.acquire();
                res = this.client.index(indexRequest);
                this.available.release();
                this.totalIdsSuccessfullyCommitted.incrementAndGet();
            }
        } catch (IOException | InterruptedException e) {

            e.printStackTrace();
        }

        return res;
    }

    public void addBulkAsnc(List<Cluster> clusters) {

        BulkRequest bulkRequest = new BulkRequest();
     //   BulkRequest bulkLinkRequest = new BulkRequest();

//        bulkLinkRequest.timeout("100m");
//        bulkRequest.timeout("10m");

        if (CollectionUtils.isNotEmpty(clusters)) {
            for (Cluster cluster : clusters) {

                if (isValidRequest(cluster)) {

                    IndexRequest request = createIndexRequest(cluster);
                    bulkRequest.add(request);
                }
            }

            commitBulk(bulkRequest);
        }

    }

    private void commitBulk(BulkRequest bulkRequest) {
        try {

            // release will happen from listener (async)
            this.available.acquire();

            ElasticBulkClusterCreateListener listener = new ElasticBulkClusterCreateListener(bulkRequest, this);

            this.client.bulkAsync(bulkRequest, listener);

            this.totalIdsProcessed.addAndGet(bulkRequest.numberOfActions());

            log.debug("Bulk insert will be created asynchronously");
        } catch (InterruptedException e) {
            log.error("Failed to acquire semaphore, lost bulk insert!", e);
        }
    }

    public long getTotalDocsCount() throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(0);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest searchRequest = new SearchRequest(this.indexName);
        searchRequest.source(sourceBuilder);

        final SearchResponse search = this.client.search(searchRequest);
        return search.getHits().getTotalHits();

    }

    public void retryAddBulk(BulkRequest bulkRequest, ElasticBulkClusterCreateListener listener) {
        try {
            // Release to give chance for other threads that waiting to execute
            this.available.release();
            this.available.acquire();
            this.client.bulkAsync(bulkRequest, listener);
            log.debug("Bulk insert retry");
        } catch (InterruptedException e) {
            log.error("Failed to acquire semaphore, lost bulk insert!", e);
        }
    }

    public boolean clusterExists(String node) {

        GetRequest getRequest = new GetRequest(
                this.indexName,
                this.docType,
                node);

        try {

            this.available.acquire();
            GetResponse getResponse = this.client.get(getRequest);
            this.available.release();
            if (getResponse.isExists()) {
                return true;
            }

        } catch (ElasticsearchStatusException | IOException | InterruptedException e) {
            log.error(e.toString());
        }

        return false;
    }

    public int getTotalIdsProcessed() {
        return totalIdsProcessed.get();
    }

    public int getTotalIdsSuccessfullyCommitted() {
        return totalIdsSuccessfullyCommitted.get();
    }

    private IndexRequest createIndexRequest(Cluster cluster) {

        IndexRequest indexRequest = new IndexRequest(
                this.indexName,
                this.docType,
                cluster.getNode());

        System.out.println(cluster.getInlinks().size());

        indexRequest.source(GSON.toJson(cluster), XContentType.JSON);

        return indexRequest;
    }


    private boolean isValidRequest(Cluster cluster) {
        return cluster != null && StringUtils.isNotBlank(cluster.getNode());
    }


    @Override
    public void close() throws IOException {

        if (client != null) {

            log.info("Closing RestHighLevelClient..");
            try {
                synchronized (closeLock) {

                    while (this.totalIdsProcessed.get() != 0) {
                        log.info("Waiting for " + this.totalIdsProcessed.get() + " async requests to complete...");
                        closeLock.wait();
                    }

                    client.close();
                }

            } catch (InterruptedException e) {
                client.close();
            }
        }
    }
}
