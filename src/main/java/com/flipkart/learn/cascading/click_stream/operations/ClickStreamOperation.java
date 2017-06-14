package com.flipkart.learn.cascading.click_stream.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.click_stream.ClickStreamFlow;
import com.flipkart.learn.cascading.click_stream.model.CAEvent;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by subhadeep.m on 13/06/17.
 */
public class ClickStreamOperation extends BaseOperation implements Function, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ClickStreamOperation.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String appclient = "androidapp";

    public ClickStreamOperation(int numInputFields, Fields outputFields) {
        super(numInputFields, outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (arguments == null) {
            logger.debug("arguments is null");
            return;
        }

        String caEventJsonString = arguments.getString(ClickStreamFlow.CaEventJson);
        if (caEventJsonString == null || caEventJsonString.isEmpty()) {
            logger.debug("CAEvent json string is null or empty");
            return;
        }

        CAEvent caEvent;
        try {
            caEvent = mapper.readValue(caEventJsonString, CAEvent.class);

        } catch (IOException e) {
            logger.debug("Error parsing the json string");
            logger.debug(e.getMessage());
            return;

        }

        // drop if this event is not from android app
        if (!caEvent.getSourceClient().toLowerCase().equals(appclient)) {
            return;
        }

        // if filter or sortby is present drop this CAEvent
        if (caEvent.getFacets() != null || caEvent.getSortby() != null) {
            return;
        }

        if (caEvent.getFdpProductClickObject() == null
                || caEvent.getFdpProductClickObject().getFdpProductClickList() == null
                || caEvent.getFdpProductClickObject().getFdpProductClickList().isEmpty()) {

            Tuple tuple = new Tuple();
            tuple.add(caEvent.getOriginalQuery().toLowerCase());
            tuple.add(caEvent.getTimestamp());
            tuple.add("nil");
            tuple.add(-1);
            functionCall.getOutputCollector().add(tuple);
            return;
        }

        for (CAEvent.ProductClick productClick : caEvent.getFdpProductClickObject()
                .getFdpProductClickList()) {
            Tuple tuple = new Tuple();
            tuple.add(caEvent.getOriginalQuery().toLowerCase());
            tuple.add(caEvent.getTimestamp());
            tuple.add(productClick.getProductId());
            tuple.add(productClick.getPosition());
            functionCall.getOutputCollector().add(tuple);
        }
    }
}
