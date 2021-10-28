/**
 * @author  Alon Eirew
 */

package wiki.elastic;

import com.google.gson.Gson;
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
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import wiki.data.WikiDataParsedPage;
import wiki.data.WikipediaParsedPage;
import wiki.data.WikipediaParsedPageRelations;
import wiki.data.obj.ReferenceContext;
import wiki.utils.WikiToElasticConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
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
    private final String docType;

    public ElasticAPI(WikiToElasticConfiguration configuration) throws IOException {
        if(configuration.getIndexName() != null && !configuration.getIndexName().isEmpty() &&
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
                                configuration.getScheme())));
    }

    public DeleteIndexResponse deleteIndex() throws ConnectException {
        DeleteIndexResponse deleteIndexResponse = null;
        try {
            DeleteIndexRequest delRequest = new DeleteIndexRequest(this.indexName);
            this.available.acquire();
            deleteIndexResponse = this.client.indices().delete(delRequest);
            this.available.release();
            LOGGER.info("Index " + this.indexName + " deleted successfully: " + deleteIndexResponse.isAcknowledged());
        } catch (ElasticsearchException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                LOGGER.info("Index " + this.indexName + " not found");
            } else {
                LOGGER.debug(ese);
            }
        } catch (ConnectException e) {
            LOGGER.error("Could not connect to elasticsearch...");
            throw e;
        } catch (IOException | InterruptedException e) {
            LOGGER.debug(e);
        }

        return deleteIndexResponse;
    }

    public CreateIndexResponse createIndex(WikiToElasticConfiguration configuration) throws IOException {
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
            if(settingFileContent != null && !settingFileContent.isEmpty()) {
                builder.loadFromSource(settingFileContent, XContentType.JSON);
            }
            crRequest.settings(builder);

            // Create index mapping
            String mappingFileContent = configuration.getMappingFileContent();
            if(mappingFileContent != null && !mappingFileContent.isEmpty()) {
                crRequest.mapping(configuration.getDocType(), mappingFileContent, XContentType.JSON);
            }

            this.available.acquire();
            createIndexResponse = this.client.indices().create(crRequest);
            this.available.release();

            LOGGER.info("Index " + configuration.getIndexName() + " created successfully: " + createIndexResponse.isAcknowledged());
        } catch (InterruptedException e) {
            LOGGER.error("Could not creat elasticsearch index");
        }

        return createIndexResponse;
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

    public void addDocAsnc(WikipediaParsedPage page) {
        if(isValidRequest(page)) {
            IndexRequest indexRequest = createIndexRequest(page);

            try {
                this.available.acquire();
                ElasticDocCreateListener listener = new ElasticDocCreateListener(indexRequest, this);
                this.client.indexAsync(indexRequest, listener);
                this.totalIdsProcessed.incrementAndGet();
                LOGGER.trace("Doc with Id " + page.getId() + " will be created asynchronously");
            } catch (InterruptedException e) {
                LOGGER.debug(e);
            }
        }
    }

    public void retryAddDoc(IndexRequest indexRequest, ElasticDocCreateListener listener) {
        try {
            // Release to give chance for other threads that waiting to execute
            this.available.release();
            this.available.acquire();
            this.client.indexAsync(indexRequest, listener);
            LOGGER.trace("Doc with Id " + indexRequest.id() + " will retry asynchronously");
        } catch (InterruptedException e) {
            LOGGER.debug(e);
        }
    }

    public IndexResponse addDoc(WikipediaParsedPage page) {
        IndexResponse res = null;

        try {
            if(isValidRequest(page)) {
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

    public void addBulkAsnc(List<WikipediaParsedPage> pages) {
        BulkRequest bulkRequest = new BulkRequest();

        if(pages != null) {
            for (WikipediaParsedPage page : pages) {
                if (isValidRequest(page)) {
                    IndexRequest request = createIndexRequest(page);
                    bulkRequest.add(request);
                }
            }

            commitBulk(bulkRequest);
        }
    }

    public void updateBulkWikidataAsnc(List<WikiDataParsedPage> pages) {
        BulkRequest bulkRequest = new BulkRequest();

        if(pages != null) {
            for (WikiDataParsedPage page : pages) {
                if (isValidWikidataRequest(page)) {
                    UpdateRequest request = createUpdateRequest(page);
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
            ElasticBulkDocCreateListener listener = new ElasticBulkDocCreateListener(bulkRequest, this);
            this.client.bulkAsync(bulkRequest, listener);
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

    public void mapInlinks(int totalAmountToExtract) throws IOException, InterruptedException {

        LOGGER.info("Reading all Wikipedia titles...");

        long totalDocsCount = this.getTotalDocsCount();
        final Scroll scroll = new Scroll(TimeValue.timeValueHours(15L));

        SearchResponse searchResponse = createElasticSearchResponse(scroll);

        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        int count = 0;
        while (searchHits != null && searchHits.length > 0) {

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);

            searchResponse = this.client.searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();

            Map<String,WikipediaParsedPage> pageMap = this.getNextScrollResults(searchHits);

            for(WikipediaParsedPage page: pageMap.values()){

                Set<ReferenceContext> referenceContexts = page.getRelations().getReferenceContexts();
                for(ReferenceContext referenceContext: referenceContexts){

//                    if("100 metres".equals(referenceContext.getTitle())) {

                        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                        sourceBuilder.query(QueryBuilders.matchPhraseQuery("title.near_match", referenceContext.getTitle()));
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
                            Set<ReferenceContext> inlinks = childPage.getRelations().getInLinks();

                            referenceContext.setTitle(page.getTitle());
                            inlinks.add(referenceContext);

                            UpdateRequest updateRequest = new UpdateRequest(
                                    this.indexName,
                                    this.docType,
                                    String.valueOf(childPage.getId()));

                            Map<String, WikipediaParsedPageRelations> wikipediaRelations = Collections.singletonMap("relations", childPage.getRelations());
                            updateRequest.doc(GSON.toJson(wikipediaRelations), XContentType.JSON);

                            //this.available.acquire();

                            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                            UpdateResponse response = this.client.update(updateRequest);
                            System.out.println(response.getGetResult());
                            //this.available.release();

                        }

                    }

//                }

            }

            if(count % 10000 == 0) {
                LOGGER.info((totalDocsCount - count) + " documents to go");
            }

            if(totalAmountToExtract > 0 && count >= totalAmountToExtract) {
                break;
            }

            count += searchHits.length;
            searchHits = searchResponse.getHits().getHits();

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
            if(count % 10000 == 0) {
                LOGGER.info((totalDocsCount - count) + " documents to go");
            }

            if(totalAmountToExtract > 0 && count >= totalAmountToExtract) {
                break;
            }

            count += searchHits.length;
            searchHits = searchResponse.getHits().getHits();
        }

        return allWikipediaIds;
    }

    private WikipediaParsedPage getPageFromHit(SearchHit hit){

        final long id = Long.parseLong(hit.getId());

        WikipediaParsedPage page = GSON.fromJson(hit.getSourceAsString(),WikipediaParsedPage.class);
        return new WikipediaParsedPage(page.getTitle(),
                id,page.getText(),page.getRedirectTitle(),page.getRelations());

    }

    private Map<String, WikipediaParsedPage> getNextScrollResults(SearchHit[] searchHits) {
        Map<String, WikipediaParsedPage> wikiPairs = new HashMap<>();
        for (SearchHit hit : searchHits) {
            final long id = Long.parseLong(hit.getId());

            WikipediaParsedPage page = GSON.fromJson(hit.getSourceAsString(),WikipediaParsedPage.class);
            wikiPairs.put(page.getTitle(),new WikipediaParsedPage(page.getTitle(),
                    id,page.getText(),page.getRedirectTitle(),page.getRelations()));

//            final Map map = hit.get();
//            final String title = (String) map.get("title");
//            String redirect = (String) map.get("redirectTitle");
//
//            Gson gson = new Gson();
//            String relJSON = gson.toJson(map.get("relations"));
//
//
//            WikipediaParsedPageRelations rel =
//
//            Map relationMap = (HashMap)map.get("relations");
//            String infobox = (String)relationMap.get("infobox");
//            boolean isPartName = (boolean)relationMap.get("isPartName");
//            boolean isDisambiguation = (boolean)relationMap.get("isDisambiguation");
//            Set<String> disambiguationLinks = (Set<String>)relationMap.get("disambiguationLinks");
//            Set<String> categories = new HashSet<>((ArrayList<String>)relationMap.get("categories"));
//            Set<String> titleParenthesis = new HashSet<>((ArrayList<String>)relationMap.get("titleParenthesis"));
//            Set<String> beCompRelations = new HashSet<>((ArrayList<String>)relationMap.get("beCompRelations"));
//            Set<ReferenceContext> referenceContexts = new HashSet<>((ArrayList<ReferenceContext>)relationMap.get("referenceContexts"));
//            Set<ReferenceContext> inLinks = new HashSet<>((ArrayList<ReferenceContext>)relationMap.get("inLinks"));
//
//            WikipediaParsedPageRelations relations = new WikipediaParsedPageRelations(infobox,isPartName,
//                    isDisambiguation,
//                    disambiguationLinks,categories,titleParenthesis,beCompRelations,
//                    referenceContexts, inLinks);

//            if (redirect != null && !redirect.isEmpty()) {
//                wikiPairs.put(title, new WikipediaParsedPage(title, id, null, redirect, relations));
//            } else {
//                wikiPairs.put(title, new WikipediaParsedPage(title, id, null, null, relations));
//            }

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

    private UpdateRequest createUpdateRequest(WikiDataParsedPage page) {

        UpdateRequest updateRequest = new UpdateRequest(
                this.indexName,
                this.docType,
                String.valueOf(page.getElasticPageId()));

        Map<String, WikiDataParsedPage> wikidataRelations = Collections.singletonMap("relations", page);
        updateRequest.doc(GSON.toJson(wikidataRelations), XContentType.JSON);

        return updateRequest;
    }

    private boolean isValidRequest(WikipediaParsedPage page) {
        return page != null && page.getId() > 0 && page.getTitle() != null && !page.getTitle().isEmpty();
    }

    private boolean isValidWikidataRequest(WikiDataParsedPage page) {
        return page != null && page.getWikipediaLangPageTitle() != null && !page.getWikipediaLangPageTitle().isEmpty();
    }

    @Override
    public void close() throws IOException {
        if(client != null) {
            LOGGER.info("Closing RestHighLevelClient..");
            try {
                synchronized(closeLock) {
                    while(this.totalIdsProcessed.get() != 0) {
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
