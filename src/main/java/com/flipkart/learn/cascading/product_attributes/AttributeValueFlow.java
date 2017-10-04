package com.flipkart.learn.cascading.product_attributes;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.product_attributes.aggregation.AttributeValueAggregator;
import com.flipkart.learn.cascading.product_attributes.operations.AttributeValueOperation;
import com.flipkart.learn.cascading.product_attributes.operations.TransformFilter;

import java.util.Map;

/**
 * Created by subhadeep.m on 17/08/17.
 */
public class AttributeValueFlow implements CascadingFlows {

    public static Fields productId = new Fields("productId");
    public static Fields productAttributesJson = new Fields("attributesJson");
    public static Fields leafPath = new Fields("leafPath");
    public static Fields attributeName = new Fields("attributeName");
    public static Fields attributeValue = new Fields("attributeValue");
    public static Fields attributeValueCount = new Fields("attributeValueCount");


    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {

        String inputPath = options.get("input");
        String outputPath = options.get("output");
        String flowName = options.get("flowName");

        Tap attributeValueTap = new GlobHfs(
                new TextDelimited(Fields.join(productId, productAttributesJson),
                        "\t"), inputPath);

        Tap attributesValueSink = new Hfs(
                new TextDelimited(Fields.join(leafPath, attributeName, attributeValue, attributeValueCount),
                        "\t"), outputPath);

        Pipe attributeValuePipe = new Pipe("attributeValuesPipe");
        attributeValuePipe = new Each(attributeValuePipe, Fields.ALL,
                new AttributeValueOperation(2, Fields.join(leafPath, attributeName,
                        attributeValue)), Fields.RESULTS);

        attributeValuePipe = new Each(attributeValuePipe, new TransformFilter());
        Pipe attributeValueGroupedPipe = new GroupBy(attributeValuePipe, Fields.join(leafPath, attributeName));
        attributeValuePipe = new Every(attributeValueGroupedPipe, Fields.ALL,
                new AttributeValueAggregator(3, Fields.join(leafPath, attributeName,
                        attributeValue, attributeValueCount)), Fields.RESULTS);

        return FlowDef.flowDef().setName(flowName)
                .addSource(attributeValuePipe, attributeValueTap)
                .addTailSink(attributeValuePipe, attributesValueSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }

}
