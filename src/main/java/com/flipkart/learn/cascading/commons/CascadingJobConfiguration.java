package com.flipkart.learn.cascading.commons;

import cascading.property.AppProps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class CascadingJobConfiguration {

    public static Properties getConfiguration(int numReducers) {
        Properties properties = new Properties();
        properties.setProperty("mapred.job.queue.name","search");
        properties.setProperty("io.compression.codecs","org.apache.hadoop.io.compress.SnappyCodec," +
                "org.apache.hadoop.io.compress.GzipCodec," +
                "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec");

//         loading action conf prepared by Oozie
        properties.setProperty("mapreduce.job.reduces", "200");
        //properties.setProperty("mapred.min.split.size", "536870912");
        //properties.setProperty("mapred.max.split.size", "536870912");
        //properties.setProperty("mapreduce.map.java.opts", "-Xmx2048M");
        properties.setProperty("mapreduce.reduce.java.opts", "-Xmx2048M");
        //properties.setProperty("io.sort.mb", "128");
        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            return properties;
        }
        if (!new File(actionXml).exists()) {
            return properties;
        }

        Configuration actionConf = new Configuration(false);
        actionConf.addResource(new Path("file:///", actionXml));
        for (Map.Entry<String, String> entry : actionConf) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        if(numReducers == 0)
            numReducers = 250;

        properties = new Properties();
        //Overriders if any.
        properties.setProperty("avro.mapred.ignore.inputs.without.extension", "false");

        return properties;
    }


    public static void setJobDetails(Properties properties, Class clazz, String tag, String applicationName) {
        AppProps.addApplicationTag(properties, tag);
        AppProps.setApplicationJarClass(properties, clazz);
        AppProps.setApplicationName(properties, applicationName);
    }
}
