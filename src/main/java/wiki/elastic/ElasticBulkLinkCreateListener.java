package wiki.elastic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.concurrent.atomic.AtomicInteger;

public class ElasticBulkLinkCreateListener implements ActionListener<BulkResponse> {

    private final static Logger LOGGER = LogManager.getLogger(ElasticBulkLinkCreateListener.class);
    private final static int MAX_RETRY = 3;

    private final AtomicInteger count =  new AtomicInteger();
    private final ElasticAPI elasicApi;
    private final BulkRequest bulkRequest;

    public ElasticBulkLinkCreateListener(BulkRequest bulkRequest, ElasticAPI elasicApi) {
        this.bulkRequest = bulkRequest;
        this.elasicApi = elasicApi;
    }

    @Override
    public void onResponse(BulkResponse bulkResponse) {

        StringBuilder sb = new StringBuilder();
        sb.append("Bulk Created/Updated done successfully, ids: [");
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE ||
                    bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                String id = itemResponse.getId();
                if (itemResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    sb.append(id).append(";");
                } else if (itemResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    sb.append(id).append(";");
                }
            }
        }
        sb.append("]");

        LOGGER.debug(sb.toString());
    }

    @Override
    public void onFailure(Exception e) {

        LOGGER.error("Failed to commit some pages with exception=" + e.getMessage());
        if (count.incrementAndGet() < MAX_RETRY) {
            this.elasicApi.retryAddLinksBulk(this.bulkRequest, this);
        } else {
            LOGGER.error("Failed, max retry exceeded, throwing request!");
        }

    }

}
