package wiki.data;

import wiki.data.relations.ExtractorsManager;
import wiki.utils.parsers.WikiPageParser;

import java.util.HashSet;

public class WikipediaParsedPageRelationsBuilder {
    private boolean isPartName = false;

    private final ExtractorsManager extractorsManager = new ExtractorsManager();

    public WikipediaParsedPageRelations build() {
        return new WikipediaParsedPageRelations(
                this.extractorsManager.getInfoboxExtrator().getResult(),
                this.isPartName,
                this.extractorsManager.getCategoryExtractor().isDisambiguation(),
                this.extractorsManager.getPairExtractor().getLinks(),
                this.extractorsManager.getCategoryExtractor().getResult(),
                this.extractorsManager.getPairExtractor().getTitleParenthesis(),
                this.extractorsManager.getBeCompExtractor().getResult().getBeCompRelations(),
                this.extractorsManager.getOutLinkExtractor().getResult(),
                new HashSet<>());
    }

    public WikipediaParsedPageRelations buildFromText(String pageText) throws Exception {
        this.extractorsManager.runExtractFromPageText(pageText);

        String[] textLines = pageText.split("\n");
        this.extractorsManager.runExtractFromPageLines(textLines);

        if (!this.extractorsManager.getPartNameExtractor().getResult()) {
            this.isPartName = this.extractorsManager.getCategoryExtractor().isPartNameInCategories();
        }

        if (!this.extractorsManager.getCategoryExtractor().isDisambiguation()) { // need to replace with utils method to check in categories if disambig
            String firstParagraph = WikiPageParser.extractFirstPageParagraph(pageText);
            this.extractorsManager.runExtractorFromParagraph(firstParagraph);
        }
        return this.build();
    }
}
