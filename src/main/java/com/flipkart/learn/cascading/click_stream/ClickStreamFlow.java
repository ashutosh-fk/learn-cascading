package com.flipkart.learn.cascading.click_stream;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.*;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.click_stream.aggregation.QueryClickAggregator;
import com.flipkart.learn.cascading.click_stream.operations.ClickStreamOperation;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.porterStemmer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by subhadeep.m on 13/06/17.
 */
public class ClickStreamFlow implements CascadingFlows {

    private static final Logger logger = LoggerFactory.getLogger(ClickStreamFlow.class);
    public static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    static final Calendar calendar = new GregorianCalendar();

    public static final Fields CaEventJson = new Fields("caEventJson");

    public static final Fields canonicalQuery = new Fields("canonical_query");
    public static final Fields originalQuery = new Fields("original_query");
    public static final Fields candidateQueries = new Fields("candidate_queries");
    public static final Fields queryDate = new Fields("query_date");
    public static final Fields clickedProductId = new Fields("clicked_product_id");
    public static final Fields clickProductPosition = new Fields("clicked_product_position");

    public static final Fields querySuccess = new Fields("query_success");
    public static final Fields productClickJson = new Fields("product_clicks");

    private static List<String> getDateStringInRange(Date startDate, Date endDate) {

        if (endDate.before(startDate)) {
            logger.error("EndDate must be after startDate");
            return null;
        }

        List<Date> dateRange = new ArrayList<>();
        calendar.setTime(startDate);

        while(calendar.getTime().before(endDate)) {
            Date currDate = calendar.getTime();
            dateRange.add(currDate);
            calendar.add(Calendar.DATE, 1);
        }

        List<String> dateRangeString = new ArrayList<>(dateRange.size());
        for (Date date : dateRange) {
            dateRangeString.add(format.format(date));
        }
        return dateRangeString;
    }

    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {

        List<GlobHfs> caEventTaps = new ArrayList<>();
        String flowName = options.get("flowName");
        String inputPath = options.get("input_path");
        String outputPath = options.get("output_path");
        String startDateString = options.get("start_date");
        String endDateString = options.get("end_date");
        Date startDate;
        Date endDate;

        try {
            startDate = format.parse(startDateString);
            endDate = format.parse(endDateString);

        } catch (ParseException e) {
            logger.error("Error in parsing date string");
            logger.error(e.getMessage());
            return null;
        }

        List<String> dateRanges = getDateStringInRange(startDate, endDate);
        if (dateRanges == null) {
            logger.error("No date range could be formed, will quit");
            return null;
        }
        for (String date : dateRanges) {
            String inputFilePath = inputPath + "/" + date + "/part-r-*";
            GlobHfs tap = new GlobHfs(new TextLine(CaEventJson), inputFilePath);
            caEventTaps.add(tap);
        }

        List<Pipe> eventProcessPipes = new ArrayList<>();
        for (GlobHfs tap : caEventTaps) {
            Pipe eventProcessPipe = new Pipe("pipe::" + tap.getIdentifier());
            eventProcessPipe = new Each(eventProcessPipe, Fields.ALL,
                    new ClickStreamOperation(1,
                            Fields.join(canonicalQuery, originalQuery, queryDate,
                                    clickedProductId, clickProductPosition)),
                    Fields.RESULTS);

            eventProcessPipes.add(eventProcessPipe);
        }

        Pipe[] eventProcessPipeArr = eventProcessPipes.toArray(new Pipe[0]);
        Pipe eventProcessMergedPipe = new Merge("eventMergedPipe", eventProcessPipeArr);
        Pipe eventQueryGroupPipe = new GroupBy(eventProcessMergedPipe, canonicalQuery);

        eventQueryGroupPipe = new Every(eventQueryGroupPipe, Fields.ALL,
                new QueryClickAggregator(5,
                        Fields.join(canonicalQuery, candidateQueries,
                                querySuccess, productClickJson)),
                Fields.RESULTS);

        FlowDef flowDefinition = FlowDef.flowDef().setName(flowName);

        int idx = 0;
        for (Pipe eventProcessPipe : eventProcessPipes) {
            flowDefinition.addSource(eventProcessPipe, caEventTaps.get(idx));
            idx++;
        }
        Tap clickEventSink = new Hfs(new TextDelimited(Fields.ALL, "\t"), outputPath);
        flowDefinition.addTailSink(eventQueryGroupPipe, clickEventSink);
        flowDefinition.setAssertionLevel(AssertionLevel.VALID);
        return flowDefinition;
    }
}
