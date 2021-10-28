package wiki.data;

import lombok.Data;
import wiki.data.obj.ReferenceContext;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Data
public class WikipediaParsedPageRelations {

    private final String infobox;
    private final boolean isPartName;
    private final boolean isDisambiguation;
    private final Set<String> disambiguationLinks;
    private final Set<String> categories;
    private final Set<String> titleParenthesis;
    private final Set<String> beCompRelations;
    private final Set<ReferenceContext> referenceContexts;
    private final Set<ReferenceContext> inLinks;

    public WikipediaParsedPageRelations(String infobox, boolean isPartName, boolean isDisambiguation,
                                        Set<String> disambiguationLinks,
                                        Set<String> categories, Set<String> titleParenthesis,
                                        Set<String> beCompRelations, Set<ReferenceContext> referenceContexts, Set<ReferenceContext> inLinks) {
        this.infobox = infobox;
        this.isPartName = isPartName;
        this.isDisambiguation = isDisambiguation;

        if(isDisambiguation) {
            this.disambiguationLinks = disambiguationLinks;
        } else {
            this.disambiguationLinks = null;
        }

        this.categories = categories;
        this.titleParenthesis = titleParenthesis;
        this.beCompRelations = beCompRelations;

        this.referenceContexts = referenceContexts;
        this.inLinks = inLinks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WikipediaParsedPageRelations that = (WikipediaParsedPageRelations) o;
        return isPartName == that.isPartName &&
                isDisambiguation == that.isDisambiguation &&
                Objects.equals(infobox, that.infobox) &&
                Objects.equals(referenceContexts, that.referenceContexts) &&
                Objects.equals(disambiguationLinks, that.disambiguationLinks) &&
                Objects.equals(categories, that.categories) &&
                Objects.equals(titleParenthesis, that.titleParenthesis) &&
                Objects.equals(beCompRelations, that.beCompRelations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(infobox, isPartName, isDisambiguation, referenceContexts,
                disambiguationLinks, categories, titleParenthesis, beCompRelations);
    }
}
