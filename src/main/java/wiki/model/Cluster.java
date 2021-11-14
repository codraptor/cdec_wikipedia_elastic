package wiki.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Cluster {

    private String node;

    @SerializedName("wikipedia_titles")
    private List<Page> wikipediaTitles;

    private List<Inlink> inlinks;

}
