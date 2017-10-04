package com.flipkart.learn.cascading.product_attributes.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.product_attributes.AttributeValueFlow;
import com.flipkart.learn.cascading.product_attributes.ProductAttributeFlow;
import com.flipkart.learn.cascading.product_attributes.model.ProductAttributes;
import com.flipkart.learn.cascading.query_click_product.QueryClickProductFlow;
import com.google.common.base.Optional;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by subhadeep.m on 17/08/17.
 */
public class AttributeValueOperation extends BaseOperation implements Function, Serializable {

    private static ObjectMapper mapper = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(TransformOperation.class);

    public AttributeValueOperation(int numInputFields, Fields outputFields) {
        super(numInputFields, outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (arguments == null) {
            return;
        }
        String productId = arguments.getString(AttributeValueFlow.productId);
        if (productId == null) {
            return;
        }

        String productAttributeJson = arguments.getString(ProductAttributeFlow.productAttributesJson);
        try {
            Map<String, List<String>> attributeMap = mapper.readValue(productAttributeJson,
                    new TypeReference<Map<String, List<String>>>() {});

            ProductAttributes productAttributes = new ProductAttributes();
            productAttributes.setAttributesMap(attributeMap);
            productAttributes.setProductId(productId);


            Optional<List<String>> leafPaths = productAttributes.getLeafPaths();

            if (!leafPaths.isPresent()) {
                return;
            }

            for (String leafPath : leafPaths.get()) {

                for (Map.Entry<String, String> attributeEntry : productAttributes.getAttributesMap().entrySet()) {
                    Tuple tuple = new Tuple();
                    tuple.add(leafPath);
                    tuple.add(attributeEntry.getKey());
                    tuple.add(attributeEntry.getValue());
                    functionCall.getOutputCollector().add(tuple);
                }
            }
        }
        catch(IOException e) {
            logger.error(e.getMessage());
            return;
        }
    }
}
