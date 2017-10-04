package com.flipkart.learn.cascading.product_attributes.aggregation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.product_attributes.AttributeValueFlow;
import lombok.Data;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by subhadeep.m on 17/08/17.
 */
public class AttributeValueAggregator extends BaseOperation<AttributeValueAggregator.Context>
implements Aggregator<AttributeValueAggregator.Context> {

    private static ObjectMapper mapper = new ObjectMapper();
    private Logger logger = LoggerFactory.getLogger(AttributeValueAggregator.class);

    public AttributeValueAggregator(int numArgs, Fields fieldDeclaration) {
        super(numArgs, fieldDeclaration);
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        Context attributeContext = new Context();
        attributeContext.setLeafPath(aggregatorCall.getGroup().getString(AttributeValueFlow.leafPath));
        attributeContext.setAttributeName(aggregatorCall.getGroup().getString(AttributeValueFlow.attributeName));
        aggregatorCall.setContext(attributeContext);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        String attributeValue = aggregatorCall.getArguments().getString(AttributeValueFlow.attributeValue);
        Map<String, Integer> attributeValueCounts
                = aggregatorCall.getContext().attributeValueCounts;

        if (attributeValue == null || attributeValue.isEmpty()) {
            return;
        }

        if (attributeValueCounts.containsKey(attributeValue)) {
            attributeValueCounts.put(attributeValue, attributeValueCounts.get(attributeValue) + 1);

        }
        else {
            attributeValueCounts.put(attributeValue, 1);
        }
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {

        String leafPath = aggregatorCall.getContext().getLeafPath();
        String attributeName = aggregatorCall.getContext().getAttributeName();

        if (aggregatorCall.getContext().getAttributeValueCounts().isEmpty()) {
            return;
        }

        for (Map.Entry<String, Integer> attributeValue : aggregatorCall.getContext()
                .getAttributeValueCounts().entrySet()) {

            if (attributeValue.getValue() < 5) {
                continue;
            }

            Tuple tuple = new Tuple();
            tuple.add(leafPath);
            tuple.add(attributeName);
            tuple.add(attributeValue.getKey());
            tuple.add(attributeValue.getValue());
            aggregatorCall.getOutputCollector().add(tuple);
        }
    }

    @Data
    public static class Context {
        private String leafPath;
        private String attributeName;
        private Map<String, Integer> attributeValueCounts = new HashMap<>();
    }

}
