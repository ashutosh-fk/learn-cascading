package com.flipkart.learn.cascading.click_stream.model;

import lombok.Data;

import java.util.Date;
import java.util.TreeMap;

/**
 * Created by subhadeep.m on 13/06/17.
 */
@Data
public class QueryProductClick {

    private String query;
    private String productId;
    private Double productWeight;
    private Double querySuccess;
    private TreeMap<Date, Double> clicksByDate;

}
