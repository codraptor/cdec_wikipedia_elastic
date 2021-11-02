/**
 * @author Alon Eirew
 */

package wiki.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import wiki.data.relations.RelationType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
public class WikiToElasticConfiguration {

    private String indexName;
    private String docType;
    private String linkDocType;
    private String mapping;
    private String linkMapping;
    private String setting;
    private String host;
    private String scheme;
    private String wikipediaDump;
    private String wikidataDump;
    private String wikidataJsonOutput;
    private String linksIndexName;
    private int port;
    private int shards;
    private int replicas;
    private int insertBulkSize;
    private boolean extractRelationFields;
    private String lang;
    private boolean includeRawText;
    private List<RelationType> relationTypes;

    private transient String mappingFileContent = null;
    private transient String linkMappingFileContent = null;
    private transient String settingFileContent = null;

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

    public String getLinkDocType() {
        return linkDocType;
    }

    public void setLinkDocType(String linkDocType) {
        this.linkDocType = linkDocType;
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

    public String getWikipediaDump() {
        return wikipediaDump;
    }

    public void setWikipediaDump(String wikipediaDump) {
        this.wikipediaDump = wikipediaDump;
    }

    public String getWikidataDump() {
        return wikidataDump;
    }

    public void setWikidataDump(String wikidataDump) {
        this.wikidataDump = wikidataDump;
    }

    public String getWikidataJsonOutput() {
        return wikidataJsonOutput;
    }

    public void setWikidataJsonOutput(String wikidataJsonOutput) {
        this.wikidataJsonOutput = wikidataJsonOutput;
    }

    public String getLinksIndexName() {
        return linksIndexName;
    }

    public void setLinksIndexName(String linksIndexName) {
        this.linksIndexName = linksIndexName;
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

    public boolean isExtractRelationFields() {
        return extractRelationFields;
    }

    public void setExtractRelationFields(boolean extractRelationFields) {
        this.extractRelationFields = extractRelationFields;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public boolean isIncludeRawText() {
        return includeRawText;
    }

    public void setIncludeRawText(boolean includeRawText) {
        this.includeRawText = includeRawText;
    }

    public List<RelationType> getRelationTypes() {
        return relationTypes;
    }

    public void setRelationTypes(List<RelationType> relationTypes) {
        this.relationTypes = relationTypes;
    }

    public void setSettingFileContent(String settingFileContent) {
        this.settingFileContent = settingFileContent;
    }

    public String getLinkMappingFileContent() throws IOException {
        if (this.linkMappingFileContent == null && this.linkMapping != null) {
            this.linkMappingFileContent = IOUtils.toString(Objects.requireNonNull(
                    WikiToElasticUtils.class.getClassLoader().getResourceAsStream(this.linkMapping)), StandardCharsets.UTF_8);
        }
        return this.linkMappingFileContent;
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
