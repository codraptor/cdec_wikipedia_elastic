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

    public static void main(String[] args) throws IOException {

        WikiToElasticConfiguration config = new Gson().fromJson(new FileReader("conf.json"), WikiToElasticConfiguration.class);

        ElasticAPI elasticAPI = new ElasticAPI(config);

        JsonReader reader = new JsonReader(new InputStreamReader(WikiToElasticUtils.
                openCompressedFileInputStream("dumps/wikidata.bz2"), "UTF-8"));

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(
                new FileOutputStream(new File("results/wikidata.json")), "UTF-8"));

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
            JsonObject englishSiteLink = siteLinks.getAsJsonObject("enwiki");
            JsonObject spanishSiteLink = siteLinks.getAsJsonObject("eswiki");
            JsonObject japaneseSiteLink = siteLinks.getAsJsonObject("jawiki");
            JsonObject chineseSiteLink = siteLinks.getAsJsonObject("zhwiki");
            JsonObject teluguSiteLink = siteLinks.getAsJsonObject("tewiki");
            JsonObject hindiSiteLink = siteLinks.getAsJsonObject("hiwiki");

            JsonObject result = new JsonObject();
            result.addProperty("key",wikidataId);

            if(englishSiteLink!=null && englishSiteLink.get("title")!=null){

                JsonObject english = new JsonObject();
                english.addProperty("title",englishSiteLink.get("title").getAsString());

                Set<ReferenceContext> inlinks =
                        elasticAPI.getInlinks(englishSiteLink.get("title").getAsString());

                if(CollectionUtils.isNotEmpty(inlinks)){
                    english.add("inlinks", gson.toJsonTree(inlinks));
                }

                result.add("en",english);

                System.out.println(englishSiteLink.get("title").getAsString());
            }

            if(spanishSiteLink!=null && spanishSiteLink.get("title")!=null)
                result.addProperty("es",spanishSiteLink.get("title").getAsString());
            if(japaneseSiteLink!=null && japaneseSiteLink.get("title")!=null)
                result.addProperty("ja",japaneseSiteLink.get("title").getAsString());
            if(chineseSiteLink!=null && chineseSiteLink.get("title")!=null)
                result.addProperty("zh",chineseSiteLink.get("title").getAsString());
            if(teluguSiteLink!=null && teluguSiteLink.get("title")!=null)
                result.addProperty("te",teluguSiteLink.get("title").getAsString());
            if(hindiSiteLink!=null && hindiSiteLink.get("title")!=null)
                result.addProperty("hi",hindiSiteLink.get("title").getAsString());

            result.addProperty("event",isEvent(wikidata));
            gson.toJson(result, JsonObject.class, writer);
            writer.flush();
        }

        writer.endArray();
        writer.close();

        reader.endArray();
        reader.close();

        elasticAPI.close();

    }

}
