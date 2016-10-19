package com.objectpartners.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectpartners.aws.S3Client;
import com.objectpartners.cassandra.CassandraDataLoader;
import com.objectpartners.common.domain.RealTime911;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

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

        List<RealTime911> calls = sparkProcessor.processCassandraData();

        /*
        create a JSON object from a subset of the Spark results
         */
        LOG.info("converting Spark results to JSON");

        int limit = 4;
        int count = 0;
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

            // save the JSON string to S3
            s3Client.storeString("test-1234", "d1", s);
            LOG.info("saving JSON to S3 completed");

            // dump the bucket to see what's inside
            List<String> descs = s3Client.getBucketObjectDescriptions("test-1234");
            LOG.info("S3 bucket test-12324 holds these objects:");
            for(String desc: descs) {
                LOG.info(desc);
            }

            // try reading the JSON data back from the bucket
            String storedObject = s3Client.readS3Object("test-1234", "d1");
            LOG.info("read from S3 bucket test-1234 and key d1: \n" + storedObject);
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
