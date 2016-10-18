package com.objectpartners.spark.rt911.analysis;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.objectpartners.aws.S3Client;
import com.objectpartners.spark.rt911.common.components.Map911Call;
import com.objectpartners.spark.rt911.common.domain.CallFrequency;
import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Spark batch processing of Cassandra data
 */
public class SparkProcessor implements Serializable {
    static final long serialVersionUID = 100L;
    private static Logger LOG = LoggerFactory.getLogger(SparkProcessor.class);


    public JavaRDD<RealTime911> processFileData() {
        // set execution configuration
        SparkConf conf = new SparkConf()
                .setAppName("CassandraClient")
                .setMaster("local")
                .set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read the rt911 table and map to RealTime911 java objects
        // this custom mapping does not require the Cassandra columns
        // and Java class fields to have the same naming
        JavaRDD<RealTime911> callData = javaFunctions(sc)
                .cassandraTable("testkeyspace", "rt911")
                .map(new Map911Call());

//         *************************************************************************************************************
//         sort by frequecy on cleansed data
//         *************************************************************************************************************

        JavaRDD<String> cleansedCallTypes = callData.map(x -> (
                x.getCallType().replaceAll("\"", "").replaceAll("[-|,]", "")));
        // create pair for reduction
        JavaPairRDD<String, Integer> cpairs = cleansedCallTypes.mapToPair(s -> new Tuple2<>(s, 1)); // 1. create pairs
        JavaPairRDD<String, Integer> creduced = cpairs.reduceByKey((a, b) -> a + b); // 2. reduce by callType
        // swap pair order so we can sort on frequency
        JavaPairRDD<Integer, String> scounts = creduced.mapToPair(Tuple2::swap); // 3. swap so we can order by frequency
        JavaPairRDD<Integer, String> oscounts = scounts.sortByKey(); // 4. sort by key (frequency)
        // swap again so the key is call type, not frequency
        JavaPairRDD<String, Integer> xscounts = oscounts.mapToPair(Tuple2::swap); // 5. swap back so the key is again callType
        // get a list
        List<Tuple2<String, Integer>> cleansedList = xscounts.collect(); //6. get a List

        LOG.info("\n\n==================================CLEANSED AND SORTED BY FREQUENCY===========================\n");

        List<CallFrequency> callFrequencyList = new ArrayList<>();
        ListIterator<Tuple2<String, Integer>> cleansedListIterator = cleansedList.listIterator(cleansedList.size());
        while (cleansedListIterator.hasPrevious()) {
            Tuple2<String, Integer> r = cleansedListIterator.previous();
            LOG.info("(" + r._1 + ", " + r._2 + ")");

            // save each callType frequency
            CallFrequency callFrequency = new CallFrequency();
            callFrequency.setCalltype(r._1());
            callFrequency.setCount(r._2());
            callFrequencyList.add(callFrequency);
        }

        JavaRDD<CallFrequency> cfRDD = sc.parallelize(callFrequencyList);
        javaFunctions(cfRDD)
                .writerBuilder("testkeyspace", "calltypes", mapToRow(CallFrequency.class)).saveToCassandra();

        // mapping Cassandra to a Java object with Cassandra java functions
        JavaRDD<RealTime911> callRDD = javaFunctions(sc)
                .cassandraTable("testkeyspace", "rt911", mapRowTo(RealTime911.class))
                .select(
                        column("id").as("incidenId"),
                        column("address").as("address"),
                        column("calltype").as("callType"),
                        column("calltime").as("dateTime"),
                        column("longitude").as("longitude"),
                        column("latitude").as("latitude"),
                        column("location").as("reportedLocation")
                );

        LOG.info("callRDD count = " + callRDD.count());

        // filter by fire type
        callRDD = callRDD.filter( c -> (c.getCallType().matches("(?i:.*\\bFire\\b.*)")));
        LOG.info("callRDD count = " + callRDD.count());

        callData = callData.repartition(1);
        return callData;
    }

}
