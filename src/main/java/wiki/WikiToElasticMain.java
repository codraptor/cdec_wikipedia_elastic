/**
 * @author  Alon Eirew
 */

package wiki;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import wiki.data.relations.ExtractorsManager;
import wiki.elastic.ElasticAPI;
import wiki.elastic.IndexingAPI;
import wiki.handlers.ElasticPageHandler;
import wiki.handlers.IPageHandler;
import wiki.handlers.IndexingPageHandler;
import wiki.model.Cluster;
import wiki.model.IndexingConfiguration;
import wiki.utils.LangConfiguration;
import wiki.utils.WikiToElasticConfiguration;
import wiki.utils.WikiToElasticUtils;
import wiki.utils.parsers.WikipediaSTAXParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WikiToElasticMain {

    private static final Gson GSON = new Gson();

    public static void main(String[] args) {

        try {

            log.info("Initiating all resources...");
            IndexingConfiguration config = GSON.fromJson(new FileReader("conf/conf-indexing.json"), IndexingConfiguration.class);

            if(StringUtils.isNotBlank(config.getDataSource())) {

                log.info("Process configuration loaded");

                long startTime = System.currentTimeMillis();
                startProcess(config);
                long endTime = System.currentTimeMillis();

                long durationInMillis = endTime - startTime;
                long took = TimeUnit.MILLISECONDS.toMinutes(durationInMillis);
                log.info("Process Done, took~" + took + "min (" + durationInMillis + "ms)");

            } else {
                log.error("Data source file is not set in configuration");
            }
        } catch (FileNotFoundException e) {
        log.error("Failed to start process", e);
        } catch (IOException e) {
            log.error("I/O Error", e);
        } catch (Exception e) {
            log.error("Something went wrong..", e);
        }
    }

    /**
     * Start the main process of parsing the wikipedia dump file, create resources and handlers for executing the task
     * @param configuration <a href="https://github.com/AlonEirew/wikipedia-to-elastic/blob/master/conf.json">conf.json file</a>
     * @throws IOException
     */
    static void startProcess(IndexingConfiguration configuration) throws IOException {

        IndexingAPI indexingAPI = new IndexingAPI(configuration);
        indexingAPI.deleteIndex();
        indexingAPI.createIndex(configuration);

        IndexingPageHandler handler = new IndexingPageHandler(indexingAPI, configuration.getInsertBulkSize());

        JsonReader reader = new JsonReader(
                new InputStreamReader(
                        new FileInputStream("data/xlec-v0.json"), "UTF-8"));

        reader.beginObject();

        while (reader.hasNext()) {

            String node = reader.nextName();
            Cluster cluster = GSON.fromJson(reader, Cluster.class);
            System.out.println(node);
            System.out.println(cluster.getWikipediaTitles().size());
            cluster.setNode(node);
            handler.addCluster(cluster);

        }

        indexingAPI.close();
        reader.endObject();
        reader.close();

        log.info("*** Total id's committed=" + indexingAPI.getTotalIdsSuccessfullyCommitted());



//        try(Scanner reader = new Scanner(System.in)) {
//
//            LOGGER.info("Reading wikidump: " + configuration.getWikipediaDump());
//
//            File wikifile = new File(configuration.getWikipediaDump());
//            if(wikifile.exists()) {
//                inputStream = WikiToElasticUtils.openCompressedFileInputStream(wikifile.getPath());
//                elasicApi = new ElasticAPI(configuration);
//
//                // Delete if index already exists
//                System.out.println("Would you like to clean & delete index (if exists) \"" + configuration.getIndexName() +
//                        "\" or update (new pages) in it [D(Delete)/U(Update)]");
//
//                // Scans the next token of the input as an int.
//                //String ans = reader.nextLine();
//                String ans = "delete";
//
//                if(ans.equalsIgnoreCase("d") || ans.equalsIgnoreCase("delete")) {
//                    elasicApi.deleteIndex();
//
//                    // Create the elastic search index
//                    elasicApi.createIndex(configuration);
//                    mode = WikipediaSTAXParser.DeleteUpdateMode.DELETE;
//                } else if(ans.equalsIgnoreCase("u") || ans.equalsIgnoreCase("update")) {
//                    if(!elasicApi.isIndexExists()) {
//                        LOGGER.info("Index \"" + configuration.getIndexName() +
//                                "\" not found, exit application.");
//                        return;
//                    }
//
//                    mode = WikipediaSTAXParser.DeleteUpdateMode.UPDATE;
//                } else {
//                    return;
//                }
//
//                // Start parsing the xml and adding pages to elastic
//                pageHandler = new ElasticPageHandler(elasicApi, configuration.getInsertBulkSize());
//
//                parser = new WikipediaSTAXParser(pageHandler, configuration, langConfiguration, mode);
//                parser.parse(inputStream);
//
//
//
//            } else {
//                LOGGER.error("Cannot find dump file-" + wikifile.getAbsolutePath());
//            }
//
//        } catch (IOException ex) {
//            LOGGER.error("Export Failed!", ex);
//        } finally {
//            if (inputStream != null) {
//                inputStream.close();
//            }
//            if(parser != null) {
//                parser.close();
//                pageHandler.close();
//                LOGGER.info("*** Total id's extracted=" + parser.getTotalIds().size());
//                LOGGER.info("*** In commit queue=" + ((ElasticPageHandler) pageHandler).getPagesQueueSize() + " (should be 0)");
//            }
//            if(elasicApi != null) {
//
////                try {
////                    elasicApi.mapInlinks(elasicApi.getTotalIdsSuccessfullyCommitted());
////                } catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
//                elasicApi.close();
//                LOGGER.info("*** Total id's committed=" + elasicApi.getTotalIdsSuccessfullyCommitted());
//            }
//        }
    }
}
