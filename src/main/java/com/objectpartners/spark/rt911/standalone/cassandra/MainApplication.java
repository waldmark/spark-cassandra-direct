package com.objectpartners.spark.rt911.standalone.cassandra;

public class MainApplication {

    public static void main(String[] args) {
        SparkProcessor sparkProcessor = new SparkProcessor();
        sparkProcessor.processFileData();
    }
}

