package com.flipkart.learn.cascading.commons;

import com.flipkart.learn.cascading.assemblyjoins.AssembledJoinsFlow;
import com.flipkart.learn.cascading.click_stream.ClickStreamFlow;
import com.flipkart.learn.cascading.data_selection.DataSelectionFlow;
import com.flipkart.learn.cascading.group_aggregation.GroupAggregatorFlow;
import com.flipkart.learn.cascading.pass_through.PassThroughFlow;
import com.flipkart.learn.cascading.plain_copier.PlainCopierFlow;
import com.flipkart.learn.cascading.product_attributes.AttributeValueFlow;
import com.flipkart.learn.cascading.product_attributes.ProductAttributeFlow;
import com.flipkart.learn.cascading.projection_selection.ProjectionSelectionFlow;
import com.flipkart.learn.cascading.query_click_product.QueryClickProductFlow;
import com.flipkart.learn.cascading.various_joins.VariousJoinsFlow;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class CascadingFlowFactory {

    public static CascadingFlows getCascadingFlow(String flowName) {
        switch (flowName) {
            case "attributeValues":
                return new AttributeValueFlow();
            case "clickStream":
                return new ClickStreamFlow();
            case "queryClickProducts":
                return new QueryClickProductFlow();
            case "productAttributes":
                return new ProductAttributeFlow();
            case "sampleFileCopy":
                return new PlainCopierFlow();
            case "projection-selection":
                return new ProjectionSelectionFlow();
            case "group-aggregator":
                return new GroupAggregatorFlow();
            case "various-joins":
                return new VariousJoinsFlow();
            case "assembled-joins":
                return new AssembledJoinsFlow();
            case "pass-through":
                return new PassThroughFlow();
            case "data-selection":
                return new DataSelectionFlow();
            default:
                throw new IllegalArgumentException("Appropriate factory is not available for this runtime configuration in Bucket:");
        }
    }
}
