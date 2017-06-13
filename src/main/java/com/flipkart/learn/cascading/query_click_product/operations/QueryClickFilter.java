package com.flipkart.learn.cascading.query_click_product.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.query_click_product.QueryClickProductFlow;

/**
 * Created by subhadeep.m on 12/06/17.
 */
public class QueryClickFilter extends BaseOperation implements Filter {

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry arguments = filterCall.getArguments();

        Double productDecayedClicks = arguments.getDouble(QueryClickProductFlow.productDecayedClicks);
        Double queryDecayedClicks = arguments.getDouble(QueryClickProductFlow.queryDecayedClicks);

        // if product decayed clicks or query decayed clicks is sufficiently small
        // remove this query-product-click
        if (productDecayedClicks < 1e-10 || queryDecayedClicks < 1e-10) {
            return true;
        }
        return false;
    }
}
