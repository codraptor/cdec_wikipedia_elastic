/**
 * @author Alon Eirew
 */

package wiki.handlers;

import org.apache.commons.lang3.StringUtils;
import wiki.elastic.IndexingAPI;
import wiki.model.Cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexingPageHandler {

    private final IndexingAPI indexingAPI;
    private final int bulkSize;

    private final Object lock = new Object();

    private final List<Cluster> clusters = new ArrayList<>();

    public IndexingPageHandler(IndexingAPI indexingAPI, int bulkSize) {
        this.indexingAPI = indexingAPI;
        this.bulkSize = bulkSize;
    }

    public boolean clusterExists(String nodeId) {

        if (StringUtils.isNotBlank(nodeId)) {
            return this.indexingAPI.clusterExists(nodeId);
        }

        return false;
    }

    /**
     * Add cluster to the handler queue, once queue is full (configuration in conf.json) the queue is persisted to elastic
     * and cleared
     * @param cluster
     */
    public void addCluster(Cluster cluster) {
        synchronized (this.lock) {
            if (cluster != null) {
                indexingAPI.addCluster(cluster);
                //clusters.add(cluster);
                //if (this.clusters.size() == this.bulkSize) {
                //    flush();
                //}
            }
        }
    }

    public void flush() {
        synchronized (this.lock) {
            if (this.clusters.size() > 0) {
                List<Cluster> copyClusters = new ArrayList<>(this.clusters);
                this.clusters.clear();
                indexingAPI.addBulkAsnc(copyClusters);
            }
        }
    }

    public int getPagesQueueSize() {
        return this.clusters.size();
    }

    public void close() throws IOException {
        List<Cluster> copyClusters = new ArrayList<>(this.clusters);
        this.clusters.clear();
        for (Cluster cluster : copyClusters) {
            indexingAPI.addCluster(cluster);
        }
    }
}
