/**
 * @author Alon Eirew
 */

package wiki.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import wiki.data.relations.RelationType;
import wiki.utils.WikiToElasticUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
public class IndexingConfiguration {

    private String indexName;
    private String docType;
    private String mapping;
    private String setting;
    private String host;
    private String scheme;
    private String dataSource;
    private int port;
    private int shards;
    private int replicas;
    private int insertBulkSize;
    private transient String mappingFileContent = null;
    private transient String settingFileContent = null;

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public String getMapping() {
        return mapping;
    }

    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    public String getSetting() {
        return setting;
    }

    public void setSetting(String setting) {
        this.setting = setting;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getShards() {
        return shards;
    }

    public void setShards(int shards) {
        this.shards = shards;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public int getInsertBulkSize() {
        return insertBulkSize;
    }

    public void setInsertBulkSize(int insertBulkSize) {
        this.insertBulkSize = insertBulkSize;
    }

    public void setSettingFileContent(String settingFileContent) {
        this.settingFileContent = settingFileContent;
    }

    public String getMappingFileContent() throws IOException {
        if (this.mappingFileContent == null && this.mapping != null) {
            this.mappingFileContent = IOUtils.toString(Objects.requireNonNull(
                    WikiToElasticUtils.class.getClassLoader().getResourceAsStream(this.mapping)), StandardCharsets.UTF_8);
        }
        return this.mappingFileContent;
    }

    public String getSettingFileContent() throws IOException {
        if (this.settingFileContent == null && this.setting != null) {
            this.settingFileContent = IOUtils.toString(Objects.requireNonNull(
                    WikiToElasticUtils.class.getClassLoader().getResourceAsStream(this.setting)), StandardCharsets.UTF_8);
        }
        return this.settingFileContent;
    }
}
