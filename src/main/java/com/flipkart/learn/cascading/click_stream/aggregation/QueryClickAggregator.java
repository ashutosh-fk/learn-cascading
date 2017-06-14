package com.flipkart.learn.cascading.click_stream.aggregation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.click_stream.ClickStreamFlow;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by subhadeep.m on 14/06/17.
 */
public class QueryClickAggregator extends BaseOperation<QueryClickAggregator.Context>
        implements Aggregator<QueryClickAggregator.Context> {

    public QueryClickAggregator(int numArgs, Fields fieldDeclaration) {
        // numArgs : Number of incoming fields that you will be sending (input field count)
        // fieldDeclaration:  The list of fields in the output.
        super(numArgs, fieldDeclaration);
    }


    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        Context queryContext = new Context();
        String query = aggregatorCall.getGroup().getString(ClickStreamFlow.query);
        queryContext.setQuery(query);
        aggregatorCall.setContext(queryContext);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        String clickProductId = aggregatorCall.getArguments().getString(ClickStreamFlow.clickedProductId);
        Integer clickProductPos = aggregatorCall.getArguments().getInteger(ClickStreamFlow.clickProductPosition);

        Context context = aggregatorCall.getContext();
        Map<String, Double> productClickWeights = context.getProductClickWeights();
        Map<String, List<Integer>> productClickPositions = context.getProductClickPositions();

        // no product clicks found
        if (clickProductId.equals("nil") && clickProductPos == -1) {
            context.setQueryFailure(context.getQueryFailure() + 1);
            return;
        }

        // product clicked, query success
        context.setQuerySuccess(context.getQuerySuccess() + 1);
        if (productClickWeights.containsKey(clickProductId)) {
            productClickWeights.put(clickProductId, productClickWeights.get(clickProductId) + 1);
        }
        else {
            productClickWeights.put(clickProductId, (double)1);
        }

        if (productClickPositions.containsKey(clickProductId)) {
            List<Integer> clickPos = productClickPositions.get(clickProductId);
            clickPos.add(clickProductPos);
        }
        else {
            List<Integer> clickPos = new ArrayList<>();
            clickPos.add(clickProductPos);
            productClickPositions.put(clickProductId, clickPos);
        }
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        Map<String, Double> productClickWeights = aggregatorCall.getContext().getProductClickWeights();
        Map<String, List<Integer>> productClickPos = aggregatorCall.getContext().getProductClickPositions();
        List<ProductClick> productClicks = new ArrayList<>();

        for (Map.Entry<String, Double> entry : productClickWeights.entrySet()) {
            ProductClick productClick = new ProductClick();
            productClick.setProductId(entry.getKey());
            productClick.setWeight(entry.getValue());
            productClick.setClickPostions(productClickPos.get(entry.getKey()));
            productClicks.add(productClick);
        }

        Double querySuccess = (aggregatorCall.getContext().getQuerySuccess() + 1d) /
                (aggregatorCall.getContext().getQuerySuccess()
                        + aggregatorCall.getContext().getQueryFailure() + 100d);

        Tuple tuple = new Tuple();
        tuple.add(aggregatorCall.getContext().getQuery());
        tuple.add(querySuccess);
        tuple.add(productClicks.toString());
        aggregatorCall.getOutputCollector().add(tuple);
    }

    @Data
    public static class Context {

        private String query;
        private Integer querySuccess = 0;
        private Integer queryFailure = 0;
        private Map<String, Double> productClickWeights = new HashMap<>();
        private Map<String, List<Integer>> productClickPositions = new HashMap<>();
    }

    @Data
    public static class ProductClick {
        private String productId;
        private Double weight;
        private List<Integer> clickPostions;
    }

}
