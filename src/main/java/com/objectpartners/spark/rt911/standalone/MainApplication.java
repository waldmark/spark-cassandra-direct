package com.objectpartners.spark.rt911.standalone;

import com.objectpartners.cassandra.data.CassandraDataLoader;
import com.objectpartners.spark.rt911.analysis.SparkProcessor;

public class MainApplication {

    public static void main(String[] args) {
        // initialize Cassandra with 911 call data
        // this requires that Cassandra is up and running
        CassandraDataLoader dataLoader = new CassandraDataLoader();
        dataLoader.insertCalls();

        // now read data from Cassandra into Spark and batch process the data
        SparkProcessor sparkProcessor = new SparkProcessor();
        sparkProcessor.processFileData();
    }
}

