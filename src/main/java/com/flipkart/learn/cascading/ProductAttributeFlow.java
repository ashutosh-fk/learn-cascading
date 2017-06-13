package com.flipkart.learn.cascading.product_attributes;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.product_attributes.operations.TransformOperation;

import java.util.Map;

/**
 * Created by subhadeep.m on 06/06/17.
 */
public class ProductAttributeFlow implements CascadingFlows {

    public static Fields productId = new Fields("productId");
    public static Fields productAttributesJson = new Fields("attributesJson");
    public static Fields leafPath = new Fields("leafPath");
    public static Fields attributeNames = new Fields("attributeNames");


    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {

        String inputPath = options.get("input");
        String outputPath = options.get("output");
        String flowName = options.get("flowName");

        Tap productAttributesTap = new Hfs(
                new TextDelimited(Fields.join(productId, productAttributesJson),
                        "\t"), inputPath);

        Tap productAttributesSink = new Hfs(
                new TextDelimited(Fields.join(productId, leafPath, attributeNames), "\t"),
                outputPath);

        Pipe productAttributePipe = new Pipe("productAttributePipe");
        productAttributePipe = new Each(productAttributePipe, Fields.ALL,
                new TransformOperation(2, Fields.join(productId, leafPath, attributeNames)),
                Fields.RESULTS);

        return FlowDef.flowDef().setName(flowName)
                .addSource(productAttributePipe, productAttributesTap)
                .addTailSink(productAttributePipe, productAttributesSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }
}
