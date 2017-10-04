package com.flipkart.learn.cascading.product_attributes.model;

import com.google.common.base.Optional;
import lombok.*;

import java.util.*;


/**
 * Created by subhadeep.m on 06/06/17.
 */
public class ProductAttributes {

    public enum IllegalFields {
        category_path ("category_path"),
        primary_path("primary_path"),
        leafPaths ("leafPaths"),
        title ("title"),
        vertical ("vertical");

        private String fieldName;

        IllegalFields(String fieldName) {
            this.fieldName = fieldName;
        }

    }

    @Getter @Setter @NonNull
    private String productId;

    @Setter @NonNull
    private Map<String, List<String>> attributesMap;


    public Optional<List<String>> getLeafPaths() {

        if (!this.attributesMap.containsKey("leafPaths") || this.attributesMap.get("leafPaths").isEmpty()) {
            return Optional.absent();
        }

        return Optional.of(this.attributesMap.get("leafPaths"));
    }


    public Optional<List<ProductPathAttribute>> getProductPathAttributes() {

        if (!this.attributesMap.containsKey("leafPaths") || this.attributesMap.get("leafPaths").isEmpty()) {
            return Optional.absent();
        }

        List<String> leafPaths = this.attributesMap.get("leafPaths");
        List<ProductPathAttribute> productPathAttributes = new ArrayList<>(leafPaths.size());

        Set<String> attributeNames = new HashSet<>(this.attributesMap.keySet());

        // remove all the attributes not to be retained
        for (IllegalFields field : IllegalFields.values()) {
            attributeNames.remove(field.fieldName);
        }

        //attributeNames.removeAll(Arrays.asList(IllegalFields.values()));

        for (String leafPath : leafPaths) {
            productPathAttributes.add(new ProductPathAttribute(this.productId, leafPath,
                    new ArrayList<>(attributeNames)));
        }

        return Optional.of(productPathAttributes);
    }

    public Map<String, String> getAttributesMap() {

        Map<String, String> attributeMap = new HashMap<>();
        List<String> illegalFieldNames = new ArrayList<>();

        for (IllegalFields field : IllegalFields.values()) {
            illegalFieldNames.add(field.fieldName);
        }

        Set<String> illegalFieldNameSet = new HashSet<>(illegalFieldNames);
        for (Map.Entry<String, List<String>> entry : this.attributesMap.entrySet()) {
            if (!illegalFieldNameSet.contains(entry.getKey())) {

                if (entry.getValue() != null & ! entry.getValue().isEmpty()) {
                    attributeMap.put(entry.getKey(), entry.getValue().get(0));
                }
            }
        }
        return attributeMap;
    }

    @Data
    @AllArgsConstructor
    public static class ProductPathAttribute {

        private String productId;
        private String leafPath;
        private List<String> attributeNames;

    }

}
