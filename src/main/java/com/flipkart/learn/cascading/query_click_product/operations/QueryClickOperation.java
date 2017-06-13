package com.flipkart.learn.cascading.query_click_product.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.query_click_product.QueryClickProductFlow;
import com.flipkart.learn.cascading.query_click_product.model.QueryClickProduct;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Created by subhadeep.m on 07/06/17.
 */
public class QueryClickOperation extends BaseOperation implements Function, Serializable {

    private static ObjectMapper mapper = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(QueryClickOperation.class);

    public QueryClickOperation(int numInputFields, Fields outputFields) {
        super(numInputFields, outputFields);
    }


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (arguments == null) {
            logger.info("Function call arguments is empty");
            return;
        }

        String productClickJson = arguments.getString(QueryClickProductFlow.queryClickJson);
        if (productClickJson == null) {
            logger.info("click intent object is null");
            return;
        }

        try {
            QueryClickProduct queryClickProduct = mapper.readValue(productClickJson, QueryClickProduct.class);
            String query = queryClickProduct.getBestIdentifiedQuery();
            if (query == null || query.length() == 0) {
                logger.info("query is null or length = 0");
                return;
            }

            Double clicks = queryClickProduct.getCtrObject().getClicks();
            Double decayedClicks = queryClickProduct.getCtrObject().getDecayedClicks();
            Double impressions = queryClickProduct.getCtrObject().getImpressions();
            Double decayedImpressions = queryClickProduct.getCtrObject().getDecayedImpressions();

            List<QueryClickProduct.ClickProduct> clickProducts = queryClickProduct.getTopClickProducts((float)0.9);

            for (QueryClickProduct.ClickProduct clickProduct: clickProducts) {
                Tuple tuple = new Tuple();
                tuple.add(query);
                // product specific attributes
                tuple.add(clickProduct.getProductId());
                tuple.add(clickProduct.getWeight());
                tuple.add(clickProduct.getDecayedWeight());

                // query specific attributes
                tuple.add(clicks);
                tuple.add(decayedClicks);
                tuple.add(impressions);
                tuple.add(decayedImpressions);

                // interpolated product clicks
                tuple.add(0.2 * clickProduct.getWeight() + 0.8 * clickProduct.getDecayedWeight());
                // interpolated query clicks
                tuple.add(0.2 * clicks + 0.8 * decayedClicks);

                functionCall.getOutputCollector().add(tuple);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            return;
        }


    }
}
