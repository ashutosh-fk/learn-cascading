package com.flipkart.learn.cascading.product_attributes.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.query_click_product.QueryClickProductFlow;

/**
 * Created by subhadeep.m on 12/06/17.
 */
public class TransformFilter extends BaseOperation implements Filter {

    private static String booksStore = "bks";

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry arguments = filterCall.getArguments();
        String productStoreLeafPath = arguments.getString(QueryClickProductFlow.leafPath);
        String[] storePath = productStoreLeafPath.split("/");

        // filter the book store queries
        for (String storeName : storePath) {
            if (storeName.equals(booksStore)) {
                return true;
            }
        }

        return false;
    }
}
