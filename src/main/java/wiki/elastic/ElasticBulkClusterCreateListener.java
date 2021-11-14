/**
 * @author Alon Eirew
 */

package wiki.elastic;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ElasticBulkClusterCreateListener implements ActionListener<BulkResponse> {

    private final static int MAX_RETRY = 3;

    private final AtomicInteger count = new AtomicInteger();
    private final IndexingAPI indexingAPI;
    private final BulkRequest bulkRequest;

    public ElasticBulkClusterCreateListener(BulkRequest bulkRequest, IndexingAPI indexingAPI) {
        this.bulkRequest = bulkRequest;
        this.indexingAPI = indexingAPI;
    }

    @Override
    public void onResponse(BulkResponse bulkResponse) {

        this.indexingAPI.onSuccess(bulkResponse.getItems().length);

        int noops = 0;

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
                } else if (itemResponse.getResult() == DocWriteResponse.Result.NOOP) {
                    noops++;
                }
            }
        }

        sb.append("]");

        this.indexingAPI.updateNOOPs(noops);
        log.debug(sb.toString());
    }

    @Override
    public void onFailure(Exception e) {
        log.error("Failed to commit some pages with exception=" + e.getMessage());

        if (count.incrementAndGet() < MAX_RETRY) {
            this.indexingAPI.retryAddBulk(this.bulkRequest, this);
        } else {
            this.indexingAPI.onFail(bulkRequest.requests().size());
            log.error("Failed, max retry exceeded, throwing request!");
        }
    }
}
