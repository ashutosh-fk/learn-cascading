package com.flipkart.learn.cascading.click_stream.model;

import com.flipkart.learn.cascading.click_stream.ClickStreamFlow;
import com.flipkart.learn.cascading.click_stream.utils.ClickStreamUtils;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by subhadeep.m on 13/06/17.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CAEvent {

    @NonNull
    @Setter @Getter
    private String originalQuery;

    @NonNull
    @Getter @Setter
    private String sourceClient;

    @Getter @Setter
    private String sortby;

    @NonNull
    private Date timestamp;

    @Getter @Setter
    private Map<String, Object> facets;

    @Getter @Setter
    private ProductClickList fdpProductClickObject;

    // override the timestamp get and set methods
    public void setTimestamp(Long timestamp) {
        if (timestamp == null) {
            return;
        }
        this.timestamp = new Date(timestamp);
    }

    public String getTimestamp() {
        return ClickStreamFlow.format.format(this.timestamp);
    }

    public String getCanonicalQuery() {
        return ClickStreamUtils.canonicaliseQuery(this.originalQuery);
    }

    @Data
    public static class ProductClickList {
        private List<ProductClick>  fdpProductClickList;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProductClick {
        private String productId;
        private Integer position;
    }
}
