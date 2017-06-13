package com.flipkart.learn.cascading.query_click_product.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by subhadeep.m on 07/06/17.
 */

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryClickProduct {

    @NonNull
    @JsonProperty("query")
    private Map<String, Double> queries;

    @NonNull
    @JsonProperty("productWeightObjects")
    private List<ClickProduct> clickProducts;

    @NonNull
    @JsonProperty("ctrObj")
    private Ctr ctrObject;

    public String getBestIdentifiedQuery() {
        if (queries == null || queries.size() == 0) {
            return null;
        }

        Double maxValue = Collections.max(queries.values());
        for (Map.Entry<String, Double> entry : queries.entrySet()) {
            if (entry.getValue().equals(maxValue)) {
                return entry.getKey().trim().replace("+", " ");
            }
        }
        return null;
    }

    public List<ClickProduct> getTopClickProducts(float retainFraction) {

        if (retainFraction > 1) {
            return null;
        }

        List<ClickProduct> topClickProducts = new ArrayList<>(this.clickProducts);
        // reverse sort the clickProducts by decayedWeights
        Collections.sort(topClickProducts, (o1, o2) -> {
            if (o1.getDecayedWeight() > o2.getDecayedWeight()) {
                return -1;
            }
            else if (o1.getDecayedWeight() < o2.getDecayedWeight()){
                return 1;
            }
            else {
                return 0;
            }
        });

        int retainIndex = (int)(retainFraction * topClickProducts.size()) + 1;
        retainIndex = retainIndex > topClickProducts.size() ? topClickProducts.size() : retainIndex;
        return topClickProducts.subList(0, retainIndex);
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ClickProduct {

        private String productId;
        private Double weight;
        private Double decayedWeight;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Ctr {

        @JsonProperty("sessionSuccess")
        private Double clicks;

        @JsonProperty("decayedSessionSuccess")
        private Double decayedClicks;

        private Double impressions;
        private Double decayedImpressions;

    }

}


