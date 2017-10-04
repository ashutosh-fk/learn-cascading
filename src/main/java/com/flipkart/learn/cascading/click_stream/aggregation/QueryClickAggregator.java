package com.flipkart.learn.cascading.click_stream.aggregation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.click_stream.ClickStreamFlow;
import lombok.Data;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * Created by subhadeep.m on 14/06/17.
 */
public class QueryClickAggregator extends BaseOperation<QueryClickAggregator.Context>
        implements Aggregator<QueryClickAggregator.Context> {

    private static ObjectMapper mapper = new ObjectMapper();
    private Logger logger = LoggerFactory.getLogger(QueryClickAggregator.class);

    public QueryClickAggregator(int numArgs, Fields fieldDeclaration) {
        // numArgs : Number of incoming fields that you will be sending (input field count)
        // fieldDeclaration:  The list of fields in the output.
        super(numArgs, fieldDeclaration);
    }


    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        Context queryContext = new Context();
        String query = aggregatorCall.getGroup().getString(ClickStreamFlow.canonicalQuery);
        queryContext.setQuery(query);
        aggregatorCall.setContext(queryContext);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        String candidateQuery = aggregatorCall.getArguments().getString(ClickStreamFlow.originalQuery);
        String clickProductId = aggregatorCall.getArguments().getString(ClickStreamFlow.clickedProductId);
        String queryDateString = aggregatorCall.getArguments().getString(ClickStreamFlow.queryDate);
        Date queryDate;
        try {
            queryDate = ClickStreamFlow.format.parse(queryDateString);

        } catch (ParseException e) {
            logger.error("Error parsing the date string, will skip the tuple", e);
            return;
        }

        Integer clickProductPos = aggregatorCall.getArguments().getInteger(ClickStreamFlow.clickProductPosition);
        Context context = aggregatorCall.getContext();
        Map<String, Double> productClickWeights = context.getProductClickWeights();
        Map<String, List<Integer>> productClickPositions = context.getProductClickPositions();
        Map<String, TreeMap<Date, Double>> productClickWeightByDay = context.getProductClickWeightsByDay();

        // no product clicks found
        if (clickProductId.equals("nil") && clickProductPos == -1) {
            context.setQueryFailure(context.getQueryFailure() + 1);
            return;
        }
        context.getCandidateQueries().add(candidateQuery);

        if (!productClickWeightByDay.containsKey(clickProductId)) {
            TreeMap<Date, Double> productweightByDay = new TreeMap<>();
            productClickWeightByDay.put(clickProductId, productweightByDay);
            productweightByDay.put(queryDate, (double)1);
        }
        else {
            Map<Date, Double> productWeightByDay = productClickWeightByDay.get(clickProductId);
            if (productWeightByDay.containsKey(queryDate)) {
                productWeightByDay.put(queryDate, productWeightByDay.get(queryDate) + 1);
            }
            else {
                productWeightByDay.put(queryDate, (double)1);
            }

        }

        // product clicked, query success
        context.setQuerySuccess(context.getQuerySuccess() + 1);
        if (productClickWeights.containsKey(clickProductId)) {
            productClickWeights.put(clickProductId, productClickWeights.get(clickProductId) + 1);
        }
        else {
            productClickWeights.put(clickProductId, (double)1);
        }

        // store the click position
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
        Map<String, TreeMap<Date, Double>> productWeightsByDay = aggregatorCall.getContext()
                .getProductClickWeightsByDay();

        List<ProductClick> productClicks = new ArrayList<>();

        for (Map.Entry<String, Double> entry : productClickWeights.entrySet()) {
            ProductClick productClick = new ProductClick();
            productClick.setProductId(entry.getKey());
            productClick.setWeight(entry.getValue());
            productClick.setClickPositions(productClickPos.get(entry.getKey()));
            productClick.setClicksByDate(productWeightsByDay.get(entry.getKey()));
            productClicks.add(productClick);
        }

        Double querySuccess = (aggregatorCall.getContext().getQuerySuccess() + 1d) /
                (aggregatorCall.getContext().getQuerySuccess()
                        + aggregatorCall.getContext().getQueryFailure() + 100d);

        // no clicked products found for the query, will return
        if (productClicks.isEmpty()) {
            return;
        }

        try {
            Tuple tuple = new Tuple();
            tuple.add(aggregatorCall.getContext().getQuery());
            tuple.add(mapper.writeValueAsString(aggregatorCall.getContext().getCandidateQueries()));
            tuple.add(querySuccess);
            tuple.add(mapper.writeValueAsString(productClicks));
            aggregatorCall.getOutputCollector().add(tuple);

        } catch (IOException e) {
            logger.error("Error deserialising the productClicks array for the query :: "
                    + e.getMessage());
        }
    }

    @Data
    public static class Context {
        private String query;
        private Set<String> candidateQueries = new HashSet<>();
        private Integer querySuccess = 0;
        private Integer queryFailure = 0;
        private Map<String, Double> productClickWeights = new HashMap<>();
        private Map<String, TreeMap<Date, Double>> productClickWeightsByDay = new HashMap<>();
        private Map<String, List<Integer>> productClickPositions = new HashMap<>();
    }

    @Data
    public static class ProductClick {
        private String productId;
        private Double weight;
        private List<Integer> clickPositions;
        private TreeMap<Date, Double> clicksByDate;
    }

}
