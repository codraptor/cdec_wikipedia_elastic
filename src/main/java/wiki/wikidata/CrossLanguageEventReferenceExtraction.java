package wiki.wikidata;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.collections4.CollectionUtils;
import wiki.data.obj.ReferenceContext;
import wiki.elastic.ElasticAPI;
import wiki.utils.WikiToElasticConfiguration;
import wiki.utils.WikiToElasticUtils;

import java.io.*;
import java.util.Set;

public class CrossLanguageEventReferenceExtraction {

    private static boolean isEvent(JsonObject wikidata) {

        JsonObject claims = wikidata.getAsJsonObject("claims");

        if(claims==null) return false;

        Set<String> properties = claims.keySet();

        System.out.println(properties);

        Boolean hasLocation = properties.contains("P276") || properties.contains("P625");
        System.out.println("Location: " + hasLocation);

        Boolean hasTime = properties.contains("P2047") || properties.contains("P580") || properties.contains("P582")
                || properties.contains("P585");
        System.out.println("Time: " + hasTime);

        return hasLocation && hasTime;

    }

    private static void addEntry(JsonObject siteLinks, Gson gson, JsonObject result, String code) throws IOException {

        JsonObject siteLink = siteLinks.getAsJsonObject(code + "wiki");

        if(siteLink!=null && siteLink.get("title")!=null){

            WikiToElasticConfiguration config = new Gson().fromJson(new FileReader("conf/conf-" + code +".json"), WikiToElasticConfiguration.class);

            ElasticAPI elasticAPI = new ElasticAPI(config);

            JsonObject language = new JsonObject();
            language.addProperty("title",siteLink.get("title").getAsString());

            Set<ReferenceContext> inlinks =
                    elasticAPI.getInlinks(siteLink.get("title").getAsString());

            if(CollectionUtils.isNotEmpty(inlinks)){
                language.add("inlinks", gson.toJsonTree(inlinks));
            }

            result.add(code,language);

            System.out.println(siteLink.get("title").getAsString());

            elasticAPI.close();
        }

    }

    public static void main(String[] args) throws IOException {

        JsonReader reader = new JsonReader(new InputStreamReader(WikiToElasticUtils.
                openCompressedFileInputStream("dumps/wikidata.bz2"), "UTF-8"));

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(
                new FileOutputStream("results/wikidata.json"), "UTF-8"));

        writer.setIndent("  ");
        writer.beginArray();

        Gson gson = new Gson();

        reader.beginArray();
        while (reader.hasNext()) {

            JsonObject input = gson.fromJson(reader, JsonObject.class);
            JsonObject entities = input.getAsJsonObject("entities");
            String wikidataId = entities.keySet().stream().findAny().get();
            System.out.println(wikidataId);
            JsonObject wikidata = entities.getAsJsonObject(wikidataId);

            JsonObject siteLinks = wikidata.getAsJsonObject("sitelinks");

            JsonObject result = new JsonObject();
            result.addProperty("key",wikidataId);

            addEntry(siteLinks,gson,result,"en");
            addEntry(siteLinks,gson,result,"es");
            addEntry(siteLinks,gson,result,"ja");
            addEntry(siteLinks,gson,result,"te");
            addEntry(siteLinks,gson,result,"zh");
            addEntry(siteLinks,gson,result,"hi");

            result.addProperty("event",isEvent(wikidata));
            gson.toJson(result, JsonObject.class, writer);
            writer.flush();
        }

        writer.endArray();
        writer.close();

        reader.endArray();
        reader.close();

    }

}
