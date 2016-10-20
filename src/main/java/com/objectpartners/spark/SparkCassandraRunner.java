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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@PropertySource(name = "props", value = "classpath:/application.properties")
public class SparkCassandraRunner {
    private static Logger LOG = LoggerFactory.getLogger(SparkCassandraRunner.class);

    @Autowired
    private SparkProcessor sparkProcessor;

    @Autowired
    private CassandraDataLoader dataLoader;

    @Autowired
    private S3Client s3Client;

    @Value(value="${s3.bucket.name:test-bucket-1}")
    private String buckeName;

    public void runSparkStreamProcessing() {

        /*
        initialize Cassandra with 911 call data
        this requires that Cassandra is up and running
         */
        LOG.info("loading data into Cassandra " + buckeName);

        dataLoader.insertCalls();

        /*
        now read data from Cassandra into Spark and batch process the data
        */
        LOG.info("processing Cassandra data with Spark");

        JavaPairRDD<String, RealTime911> callsByCallDate = sparkProcessor.processCassandraData();

        /*
        create a JSON object from a subset of the Spark results
         */
        LOG.info("converting Spark results to JSON");

        JavaPairRDD<String, Iterable<RealTime911>> x = callsByCallDate.groupByKey();
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
                LOG.debug(rt911.getDateTime() + " " + rt911.getCallType());
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
        LOG.info("storing JSON into S3 bucket: " + buckeName);

        try {
            // remove the S3 bucket, this removes all objects in the bucket first
            s3Client.removeBucket(buckeName);
            LOG.info("S3 bucket " + buckeName + " deleted");
        } catch (Exception e) {
            // bucket not deleted, may not have been there
        }

        try {
            // create the bucket to start fresh
            s3Client.createBucket(buckeName);
            LOG.info("S3 bucket " + buckeName + " created");

            for(String key: s3BucketData.keySet()) {
                s3Client.storeString(buckeName, key, s3BucketData.get(key));
            }
            LOG.info("saving JSON to S3 completed");

            // dump the bucket to see what's inside
            List<String> descs = s3Client.getBucketObjectDescriptions(buckeName);
            LOG.info("S3 bucket " + buckeName + " holds these objects:");
            for(String desc: descs) {
                LOG.info(desc);
            }

            // try reading the JSON data back from the bucket
            String key = s3BucketData.keySet().iterator().next(); // get first key
            String storedObject = s3Client.readS3Object(buckeName, s3BucketData.keySet().iterator().next());
            LOG.info("read from S3 bucket " + buckeName + " and key " + key + ": \n" + storedObject);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            // clean up
            s3Client.removeBucket(buckeName);
        }
        LOG.info("Spark processing Cassandra data completed ....");

    }
}
