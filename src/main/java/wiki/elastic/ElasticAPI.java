/**
 * @author Alon Eirew
 */

package wiki.elastic;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.collections4.CollectionUtils;
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

public class ElasticAPI implements Closeable {

    private final static Logger LOGGER = LogManager.getLogger(ElasticAPI.class);
    private static final Gson GSON = new Gson();
    private final static int MAX_AVAILABLE = 10;

    private final AtomicInteger totalIdsProcessed = new AtomicInteger(0);
    private final AtomicInteger totalIdsSuccessfullyCommitted = new AtomicInteger(0);

    // Limit the number of threads accessing elastic in parallel
    private final Semaphore available = new Semaphore(MAX_AVAILABLE, true);
    private final RestHighLevelClient client;
    private final Object closeLock = new Object();

    private final String indexName;
    private final String linksIndexName;
    private final String docType;
    private final String linkDocType;

    public ElasticAPI(WikiToElasticConfiguration configuration) throws IOException {
        if (configuration.getIndexName() != null && !configuration.getIndexName().isEmpty() &&
                configuration.getDocType() != null && !configuration.getDocType().isEmpty()) {

            this.indexName = configuration.getIndexName();
            this.docType = configuration.getDocType();
            this.linksIndexName = configuration.getLinksIndexName();
            this.linkDocType = configuration.getLinkDocType();

        } else {
            throw new IOException("Missing mandatory values of \"indexName\" & \"docType\" in configuration");
        }

        // init elastic client
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(configuration.getHost(),
                                configuration.getPort(),
                                configuration.getScheme())));
    }

    public void deleteIndex() throws ConnectException {

        DeleteIndexResponse deleteIndexResponse;
        DeleteIndexResponse deleteLinksIndexResponse;

        try {

            DeleteIndexRequest delRequest = new DeleteIndexRequest(this.indexName);
            DeleteIndexRequest delLinksRequest = new DeleteIndexRequest(this.linksIndexName);
            this.available.acquire();
            deleteIndexResponse = this.client.indices().delete(delRequest);
            deleteLinksIndexResponse = this.client.indices().delete(delLinksRequest);
            this.available.release();
            LOGGER.info("Index " + this.indexName + " deleted successfully: " + deleteIndexResponse.isAcknowledged());
            LOGGER.info("Index " + this.linksIndexName + " deleted successfully: " + deleteLinksIndexResponse.isAcknowledged());

        } catch (ElasticsearchException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                LOGGER.info("Index " + ese.getIndex().getName() + " not found");
            } else {
                LOGGER.debug(ese);
            }
        } catch (ConnectException e) {
            LOGGER.error("Could not connect to elasticsearch...");
            throw e;
        } catch (IOException | InterruptedException e) {
            LOGGER.debug(e);
        }

    }

    public void createIndex(WikiToElasticConfiguration configuration) throws IOException {

        CreateIndexResponse createIndexResponse = null;
        CreateIndexResponse createLinksIndexResponse = null;

        try {
            // Create the index
            CreateIndexRequest crRequest = new CreateIndexRequest(configuration.getIndexName());
            CreateIndexRequest createLinksRequest = new CreateIndexRequest(configuration.getLinksIndexName());

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
            createLinksRequest.settings(builder);

            // Create index mapping
            String mappingFileContent = configuration.getMappingFileContent();
            String linkMappingFileContent = configuration.getLinkMappingFileContent();

            if (mappingFileContent != null && !mappingFileContent.isEmpty() && linkMappingFileContent!=null && !linkMappingFileContent.isEmpty()) {

                crRequest.mapping(configuration.getDocType(), mappingFileContent, XContentType.JSON);
                createLinksRequest.mapping(configuration.getLinkDocType(), linkMappingFileContent, XContentType.JSON);
            }

            this.available.acquire();
            createIndexResponse = this.client.indices().create(crRequest);
            createLinksIndexResponse = this.client.indices().create(createLinksRequest);
            this.available.release();

            LOGGER.info("Index " + configuration.getIndexName() + " created successfully: " + createIndexResponse.isAcknowledged());
            LOGGER.info("Index " + configuration.getIndexName() + " created successfully: " + createLinksIndexResponse.isAcknowledged());

        } catch (InterruptedException e) {
            LOGGER.error("Could not creat elasticsearch index");
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

    public IndexResponse addDoc(WikipediaParsedPage page) {
        IndexResponse res = null;

        try {
            if (isValidRequest(page)) {
                IndexRequest indexRequest = createIndexRequest(page);
                List<IndexRequest> linkIndexRequests = createLinkRequestList(page);
                this.available.acquire();

                if (linkIndexRequests != null) {
                    for (IndexRequest linkIndexRequest : linkIndexRequests) {
                        this.client.index(linkIndexRequest);
                    }
                }
                res = this.client.index(indexRequest);
                this.available.release();
                this.totalIdsSuccessfullyCommitted.incrementAndGet();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return res;
    }

    public void addBulkAsnc(List<WikipediaParsedPage> pages) {

        BulkRequest bulkRequest = new BulkRequest();
        BulkRequest bulkLinkRequest = new BulkRequest();

        if (pages != null) {
            for (WikipediaParsedPage page : pages) {
                if (isValidRequest(page)) {
                    IndexRequest request = createIndexRequest(page);
                    List<IndexRequest> linkRequests = createLinkRequestList(page);
                    if (linkRequests != null) {
                        linkRequests.forEach(linkRequest -> bulkLinkRequest.add(linkRequest));
                    }
                    bulkRequest.add(request);
                }
            }

            commitBulk(bulkRequest, bulkLinkRequest);
        }

    }

    private void commitBulk(BulkRequest bulkRequest, BulkRequest bulkLinkRequest) {
        try {
            // release will happen from listener (async)
            this.available.acquire();

            ElasticBulkDocCreateListener listener = new ElasticBulkDocCreateListener(bulkRequest, this);
            ElasticBulkLinkCreateListener linkListener = new ElasticBulkLinkCreateListener(bulkLinkRequest, this);

            this.client.bulkAsync(bulkRequest, listener);
            this.client.bulkAsync(bulkLinkRequest, linkListener);

            this.totalIdsProcessed.addAndGet(bulkRequest.numberOfActions());

            LOGGER.debug("Bulk insert will be created asynchronously");
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore, lost bulk insert!", e);
        }
    }

    public Set<ReferenceContext> getInlinks(String title) throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchPhraseQuery("title.near_match", title));

        sourceBuilder.from(0);
        sourceBuilder.size(1);
        sourceBuilder.timeout(new TimeValue(5, TimeUnit.MINUTES));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(this.indexName);
        searchRequest.source(sourceBuilder);

        SearchResponse child = this.client.search(searchRequest);

        SearchHit[] childHits = child.getHits().getHits();
        if (childHits != null && childHits.length > 0) {

            WikipediaParsedPage childPage = getPageFromHit(childHits[0]);
            return childPage.getRelations().getInLinks();

        } else {
            return null;
        }

    }


    public Map<String, WikipediaParsedPage> readAllWikipediaIdsTitles(int totalAmountToExtract) throws IOException {
        LOGGER.info("Reading all Wikipedia titles...");
        Map<String, WikipediaParsedPage> allWikipediaIds = new HashMap<>();

        long totalDocsCount = this.getTotalDocsCount();
        final Scroll scroll = new Scroll(TimeValue.timeValueHours(5L));
        SearchResponse searchResponse = createElasticSearchResponse(scroll);

        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        int count = 0;
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = this.client.searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();

            allWikipediaIds.putAll(this.getNextScrollResults(searchHits));
            if (count % 10000 == 0) {
                LOGGER.info((totalDocsCount - count) + " documents to go");
            }

            if (totalAmountToExtract > 0 && count >= totalAmountToExtract) {
                break;
            }

            count += searchHits.length;
            searchHits = searchResponse.getHits().getHits();
        }

        return allWikipediaIds;
    }

    private WikipediaParsedPage getPageFromHit(SearchHit hit) {

        final long id = Long.parseLong(hit.getId());

        WikipediaParsedPage page = GSON.fromJson(hit.getSourceAsString(), WikipediaParsedPage.class);
        return new WikipediaParsedPage(page.getTitle(),
                id, page.getText(), page.getRedirectTitle(), page.getRelations());

    }

    private Map<String, WikipediaParsedPage> getNextScrollResults(SearchHit[] searchHits) {
        Map<String, WikipediaParsedPage> wikiPairs = new HashMap<>();
        for (SearchHit hit : searchHits) {
            final long id = Long.parseLong(hit.getId());

            WikipediaParsedPage page = GSON.fromJson(hit.getSourceAsString(), WikipediaParsedPage.class);
            wikiPairs.put(page.getTitle(), new WikipediaParsedPage(page.getTitle(),
                    id, page.getText(), page.getRedirectTitle(), page.getRelations()));

        }

        return wikiPairs;
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

    private SearchResponse createElasticSearchResponse(Scroll scroll) throws IOException {
        final SearchRequest searchRequest = new SearchRequest(this.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQuery());
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(scroll);
        return this.client.search(searchRequest);
    }

    public void retryAddBulk(BulkRequest bulkRequest, ElasticBulkDocCreateListener listener) {
        try {
            // Release to give chance for other threads that waiting to execute
            this.available.release();
            this.available.acquire();
            this.client.bulkAsync(bulkRequest, listener);
            LOGGER.debug("Bulk insert retry");
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore, lost bulk insert!", e);
        }
    }

    public void retryAddLinksBulk(BulkRequest bulkRequest, ElasticBulkLinkCreateListener listener) {
        try {
            // Release to give chance for other threads that waiting to execute
            this.available.release();
            this.available.acquire();
            this.client.bulkAsync(bulkRequest, listener);
            LOGGER.debug("Bulk insert retry");
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore, lost bulk insert!", e);
        }
    }

    public boolean isDocExists(String docId) {
        GetRequest getRequest = new GetRequest(
                this.indexName,
                this.docType,
                docId);

        try {
            this.available.acquire();
            GetResponse getResponse = this.client.get(getRequest);
            this.available.release();
            if (getResponse.isExists()) {
                return true;
            }
        } catch (ElasticsearchStatusException | IOException | InterruptedException e) {
            LOGGER.error(e);
        }

        return false;
    }

    public boolean isIndexExists() {
        boolean ret = false;
        try {
            OpenIndexRequest openIndexRequest = new OpenIndexRequest(this.indexName);
            ret = client.indices().open(openIndexRequest).isAcknowledged();
        } catch (ElasticsearchStatusException | IOException ignored) {
        }

        return ret;
    }

    public int getTotalIdsProcessed() {
        return totalIdsProcessed.get();
    }

    public int getTotalIdsSuccessfullyCommitted() {
        return totalIdsSuccessfullyCommitted.get();
    }

    private IndexRequest createIndexRequest(WikipediaParsedPage page) {
        IndexRequest indexRequest = new IndexRequest(
                this.indexName,
                this.docType,
                String.valueOf(page.getId()));

        indexRequest.source(GSON.toJson(page), XContentType.JSON);

        return indexRequest;
    }

    private List<IndexRequest> createLinkRequestList(WikipediaParsedPage page) {

        if (CollectionUtils.isEmpty(page.getRelations().getReferenceContexts())) return null;

        return page.getRelations().getReferenceContexts()
                .parallelStream().map(reference -> {

                    IndexRequest indexRequest = new IndexRequest(
                            this.linksIndexName,
                            this.linkDocType);

                    reference.setSource(page.getTitle());
                    indexRequest.source(GSON.toJson(reference), XContentType.JSON);

                    return indexRequest;

                }).collect(Collectors.toList());
    }


    private boolean isValidRequest(WikipediaParsedPage page) {
        return page != null && page.getId() > 0 && page.getTitle() != null && !page.getTitle().isEmpty();
    }


    @Override
    public void close() throws IOException {
        if (client != null) {
            LOGGER.info("Closing RestHighLevelClient..");
            try {
                synchronized (closeLock) {
                    while (this.totalIdsProcessed.get() != 0) {
                        LOGGER.info("Waiting for " + this.totalIdsProcessed.get() + " async requests to complete...");
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
