package com.objectpartners.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectpartners.aws.S3Client;
import com.objectpartners.cassandra.CassandraDataLoader;
import com.objectpartners.common.domain.RealTime911;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SparkCassandraRunner {
    private static Logger LOG = LoggerFactory.getLogger(SparkCassandraRunner.class);

    @Autowired
    private SparkProcessor sparkProcessor;

    @Autowired
    private CassandraDataLoader dataLoader;

    @Autowired
    private S3Client s3Client;

    public void runSparkStreamProcessing() {

        /*
        initialize Cassandra with 911 call data
        this requires that Cassandra is up and running
         */
        LOG.info("loading data into Cassandra");

        dataLoader.insertCalls();

        /*
        now read data from Cassandra into Spark and batch process the data
        */
        LOG.info("processing Cassandra data with Spark");

        JavaPairRDD<String, RealTime911> callsByTimeStamp = sparkProcessor.processCassandraData();

        /*
        create a JSON object from a subset of the Spark results
         */
        LOG.info("converting Spark results to JSON");

        JavaPairRDD<String, Iterable<RealTime911>> x = callsByTimeStamp.groupByKey();
        Map<String, Iterable<RealTime911>> xmap = x.collectAsMap();
        Set<String> keys = xmap.keySet();

        ObjectMapper mapper = new ObjectMapper();

        Map<String, String> s3BucketData = new HashMap<>();
        for(String key: keys) {
            List<String> jsonArrayElements = new ArrayList<>();
            Iterable<RealTime911> iterable = xmap.get(key);
            Iterator<RealTime911> iterator = iterable.iterator();
            while(iterator.hasNext()) {
                RealTime911 rt911 = iterator.next();
                LOG.trace(rt911.getDateTime() + " " + rt911.getCallType());
                try {
                    String jsonRT911 = mapper.writeValueAsString(rt911);
                    jsonArrayElements.add(jsonRT911);
                } catch (JsonProcessingException e) {
                    LOG.error(e.getMessage());
                }
            }

            StringJoiner joiner = new StringJoiner(",");
            jsonArrayElements.forEach(joiner::add);
            s3BucketData.put(key, "[" + joiner.toString() + "]");
        }

        /*
        save to S3
         */
        LOG.info("storing JSON into S3");

        try {
            // remove the S3 bucket, this removes all objects in the bucket first
            s3Client.removeBucket("test-1234");
            LOG.info("S3 bucket created");
        } catch (Exception e) {
            // bucket not deleted, may not have been there
        }

        try {
            // create the bucket to start fresh
            s3Client.createBucket("test-1234");
            LOG.info("S3 bucket created");

            for(String key: s3BucketData.keySet()) {
                s3Client.storeString("test-1234", key, s3BucketData.get(key));
            }
            LOG.info("saving JSON to S3 completed");

            // dump the bucket to see what's inside
            List<String> descs = s3Client.getBucketObjectDescriptions("test-1234");
            LOG.info("S3 bucket test-12324 holds these objects:");
            for(String desc: descs) {
                LOG.info(desc);
            }

            // try reading the JSON data back from the bucket
            String key = s3BucketData.keySet().iterator().next(); // get first key
            String storedObject = s3Client.readS3Object("test-1234", s3BucketData.keySet().iterator().next());
            LOG.info("read from S3 bucket test-1234 and key " + key + ": \n" + storedObject);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            // clean up
            s3Client.removeObject("test-1234", "d1");
            s3Client.removeBucket("test-1234");
        }
        LOG.info("Spark processing Cassandra data completed ....");

    }
}
