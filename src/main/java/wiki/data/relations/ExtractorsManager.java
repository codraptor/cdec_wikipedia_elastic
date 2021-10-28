package wiki.data.relations;

import lombok.Data;
import lombok.Getter;
import wiki.data.obj.BeCompRelationResult;
import wiki.data.obj.ReferenceContext;
import wiki.utils.LangConfiguration;
import wiki.utils.WikiToElasticConfiguration;

import java.util.List;
import java.util.Set;

@Getter
public class ExtractorsManager {

    private static List<RelationType> relationTypes;

    private final CategoryRelationExtractor categoryExtractor = new CategoryRelationExtractor();
    private final IRelationsExtractor<Boolean> partNameExtractor = new PartNameRelationExtractor();
    private final LinkAndParenthesisRelationExtractor pairExtractor = new LinkAndParenthesisRelationExtractor();
    private final IRelationsExtractor<BeCompRelationResult> beCompExtractor = new BeCompRelationExtractor();
    private final IRelationsExtractor<String> infoboxExtrator = new InfoboxRelationExtractor();
    private final IRelationsExtractor<Set<ReferenceContext>> outLinkExtractor = new OutLinkExtractor();

    public static void initExtractors(WikiToElasticConfiguration config, LangConfiguration langConfiguration) {
        relationTypes = config.getRelationTypes();
        for(RelationType type : config.getRelationTypes()) {
            switch (type) {
                case BeComp:
                    BeCompRelationExtractor.initResources(langConfiguration);
                    break;
                case Infobox:
                    InfoboxRelationExtractor.initResources(langConfiguration);
                    break;
                case Category:
                    CategoryRelationExtractor.initResources(langConfiguration);
                    break;
                case PartName:
                    PartNameRelationExtractor.initResources(langConfiguration);
                    break;
                case Parenthesis:
                    LinkAndParenthesisRelationExtractor.initResources(langConfiguration);
                    break;
                case OutLinks:
                    break;
                default:
                    throw new IllegalArgumentException("No such relation type-" + type.name());
            }
        }
    }

    public void runExtractFromPageText(String pageText) throws Exception {
        if(relationTypes.contains(RelationType.Infobox)) {
            this.infoboxExtrator.extract(pageText);
        }
        if(relationTypes.contains(RelationType.OutLinks)) {
            outLinkExtractor.extract(pageText);
        }
    }

    public void runExtractFromPageLines(String[] lines) throws Exception {
        for (String line : lines) {
            if (relationTypes.contains(RelationType.Category)) {
                categoryExtractor.extract(line);
            }

            if (relationTypes.contains(RelationType.PartName)) {
                partNameExtractor.extract(line);
            }

            if (relationTypes.contains(RelationType.Parenthesis)) {
                pairExtractor.extract(line);
            }

        }
    }

    public void runExtractorFromParagraph(String paragraph) throws Exception {
        if(relationTypes.contains(RelationType.BeComp)) {
            this.beCompExtractor.extract(paragraph);
        }
    }
}
