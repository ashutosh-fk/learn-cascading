package com.flipkart.learn.cascading.query_click_product;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.operation.Filter;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.product_attributes.operations.TransformFilter;
import com.flipkart.learn.cascading.product_attributes.operations.TransformOperation;
import com.flipkart.learn.cascading.query_click_product.operations.QueryClickFilter;
import com.flipkart.learn.cascading.query_click_product.operations.QueryClickOperation;

import java.util.Map;

/**
 * Created by subhadeep.m on 07/06/17.
 */
public class QueryClickProductFlow implements CascadingFlows {


    public static final Fields productId1 = new Fields("productId1");
    public static final Fields productId2 = new Fields("productId2");

    public static final Fields productAttributesJson = new Fields("attributesJson");

    public static final Fields queryClickJson = new Fields("queryClickJson");
    public static final Fields queryIntentJson = new Fields("productIntentJson");

    public static final Fields leafPath = new Fields("leafPath");
    public static final Fields attributeNames = new Fields("attributeNames");
    public static final Fields query = new Fields("query");
    public static final Fields productClicks = new Fields("product_clicks");
    public static final Fields productDecayedClicks = new Fields("product_decayed_clicks");

    public static final Fields queryImpressions = new Fields("query_impressions");
    public static final Fields queryDecayedImpressions = new Fields("query_decayed_impressions");
    public static final Fields queryClicks = new Fields("query_clicks");
    public static final Fields queryDecayedClicks = new Fields("query_decayed_clicks");

    public static final Fields productInterpolatedClicks = new Fields(("product_interpolated_clicks"));
    public static final Fields queryInterpolatedClicks = new Fields(("query_interpolated_clicks"));


    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        String queryInputPath = options.get("query_input");
        String productInputPath = options.get("product_input");
        String output = options.get("output");
        String flowName = options.get("flowName");


        Tap productAttributesTap = new GlobHfs(
                new TextDelimited(Fields.join(productId1, productAttributesJson),
                        "\t"), productInputPath);


        Tap queryClicksTap = new GlobHfs(
                new TextDelimited(Fields.join(queryClickJson, queryIntentJson),
                "\t"), queryInputPath);

        Pipe productAttributePipe = new Pipe("productAttributePipe");
        productAttributePipe = new Each(productAttributePipe, Fields.ALL,
                new TransformOperation(2, Fields.join(productId1, leafPath, attributeNames)),
                Fields.RESULTS);

        productAttributePipe = new Each(productAttributePipe, new TransformFilter());

        Pipe queryClickPipe = new Pipe("queryClickPipe");
        Fields queryClickOutFields = Fields.join(
                query, productId2, productClicks, productDecayedClicks,
                queryClicks, queryDecayedClicks, queryImpressions,
                queryDecayedImpressions, productInterpolatedClicks,
                queryInterpolatedClicks);

        queryClickPipe = new Each(queryClickPipe, Fields.ALL,
                new QueryClickOperation(2, queryClickOutFields),
                Fields.RESULTS);

        queryClickPipe = new Each(queryClickPipe, new QueryClickFilter());

        Pipe queryProductClickJoinPipe = new CoGroup(
                queryClickPipe, productId2, productAttributePipe, productId1,
                new InnerJoin());

        queryProductClickJoinPipe = new Retain(queryProductClickJoinPipe,
                Fields.join(query, productId2, leafPath, queryInterpolatedClicks,
                        productInterpolatedClicks, attributeNames));

        Tap queryClicksSink = new Hfs(new TextDelimited(Fields.ALL, "\t"), output);

        return FlowDef.flowDef().setName(flowName)
                .addSource(queryClickPipe, queryClicksTap)
                .addSource(productAttributePipe, productAttributesTap)
                .addTailSink(queryProductClickJoinPipe, queryClicksSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }
}
