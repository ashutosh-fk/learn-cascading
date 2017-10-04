package com.flipkart.learn.cascading.click_stream.utils;

import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.porterStemmer;
import java.util.regex.Pattern;

/**
 * Created by subhadeep.m on 14/06/17.
 */
public class ClickStreamUtils {

    //public static final SnowballStemmer stemmer = new porterStemmer();
    public static final Pattern nonAlnumPattern = Pattern.compile("[^a-zA-Z0-9.%']+");

    public static String canonicaliseQuery(String query) {
        String[] queryTokens = query.split(nonAlnumPattern.pattern());
        //String[] canonicalQuery = new String[queryTokens.length];

        //int idx = 0;
        //for (String queryToken : queryTokens) {
        //    stemmer.setCurrent(queryToken);
        //    stemmer.stem();
        //    canonicalQuery[idx++] = stemmer.getCurrent();
        //}
        return String.join(" ", queryTokens);
    }
}
