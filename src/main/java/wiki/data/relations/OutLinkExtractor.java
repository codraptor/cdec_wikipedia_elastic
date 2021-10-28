package wiki.data.relations;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import wiki.data.obj.ReferenceContext;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OutLinkExtractor implements IRelationsExtractor<Set<ReferenceContext>> {

    private Set<ReferenceContext> referenceContexts = new HashSet<>();

    private static final String REFERENCE_REGEX = "\\[\\[(.+?)\\]\\]";
    private static final Pattern REFERENCE_PATTERN = Pattern.compile(REFERENCE_REGEX);

    @Override
    public IRelationsExtractor<Set<ReferenceContext>> extract(String pageText) throws Exception {

        List<String> contexts = Arrays.asList(pageText.split("\\n\\n"));

        for (String context : contexts) {

            Matcher matcher = REFERENCE_PATTERN.matcher(context);
            Map<String,Integer> offsetMap = new HashMap<>();

            while (matcher.find()) {

                String reference = matcher.group(1);
                List<String> strings = Arrays.asList(reference.split("\\|"));

                if (CollectionUtils.isNotEmpty(strings) && strings.size() <= 2) {

                    if (strings.size() == 2) {

                        ReferenceContext referenceContext = new ReferenceContext();
                        referenceContext.setTitle(strings.get(0));
                        referenceContext.setSpan(strings.get(1));

                        int offset = -1;

                        if(offsetMap.containsKey(strings.get(0))){
                            offset = offsetMap.get(strings.get(0));
                        }

                        offset = context.indexOf(reference, offset+1);
                        offsetMap.put(strings.get(0),offset);

                        referenceContext.setContext(context);
                        referenceContext.setOffset(offset);

                        this.referenceContexts.add(referenceContext);


                    } else if (strings.size() == 1 && StringUtils.isNotBlank(strings.get(0))) {

                        ReferenceContext referenceContext = new ReferenceContext();
                        referenceContext.setTitle(strings.get(0));
                        referenceContext.setSpan(strings.get(0));

                        int offset = -1;

                        if(offsetMap.containsKey(strings.get(0))){
                            offset = offsetMap.get(strings.get(0));
                        }

                        offset = context.indexOf(reference, offset+1);
                        offsetMap.put(strings.get(0),offset);

                        referenceContext.setContext(context);
                        referenceContext.setOffset(offset);

                        this.referenceContexts.add(referenceContext);

                    }

                }

            }

        }

        return this;
    }

    @Override
    public Set<ReferenceContext> getResult() {
        return this.referenceContexts;
    }

}
