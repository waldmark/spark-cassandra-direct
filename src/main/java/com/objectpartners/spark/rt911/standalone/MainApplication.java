package com.objectpartners.spark.rt911.standalone;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectpartners.aws.S3Client;
import com.objectpartners.cassandra.data.CassandraDataLoader;
import com.objectpartners.spark.rt911.analysis.SparkProcessor;
import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class MainApplication {

    private static Logger LOG = LoggerFactory.getLogger(MainApplication.class);

    public static void main(String[] args) {
        // initialize Cassandra with 911 call data
        // this requires that Cassandra is up and running
        CassandraDataLoader dataLoader = new CassandraDataLoader();
        dataLoader.insertCalls();

        // create an S3 bucket to hold data produced from Spark
        S3Client s3Client = new S3Client();

        try {
            s3Client.removeBucket("test-1234");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        s3Client.createBucket("test-1234");

        // now read data from Cassandra into Spark and batch process the data
        SparkProcessor sparkProcessor = new SparkProcessor();
        JavaRDD<RealTime911> callData = sparkProcessor.processFileData();

        List<RealTime911> calls = callData.collect();

        int limit = 4;
        int count = 0;
        // convert RDD to JSON objects
        ObjectMapper mapper = new ObjectMapper();
        List<String> jsonArrayElements = new ArrayList<>();
        for(RealTime911 rt911: calls) {
            try {
                String jsonRT911 = mapper.writeValueAsString(rt911);
                jsonArrayElements.add(jsonRT911);
                if(++count > limit) break;
            } catch (JsonProcessingException e) {
                LOG.info(e.getMessage());
            }
        }
        StringJoiner joiner = new StringJoiner(",");
        jsonArrayElements.forEach(joiner::add);

        String s = "[" + joiner.toString() + "]";
        s3Client.storeString("test-1234", "d1", s);

        List<String> descs = s3Client.getBucketObjectDescriptions("test-1234");
        LOG.info("S3 bucket test-12324 holds these objects:");
        for(String desc: descs) {
            LOG.info(desc);
        }

        try {
            String storedObject = s3Client.readS3Object("test-1234", "d1");
            LOG.info("read from S3 bucket test-1234 and key d1: " + storedObject);
        } catch (IOException e) {
            e.printStackTrace();
        }

        s3Client.removeObject("test-1234", "d1");
        s3Client.removeBucket("test-1234");
    }
}

