package com.objectpartners.spark.rt911.analysis;

import com.objectpartners.spark.rt911.common.components.Map911Call;
import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.ListIterator;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Spark batch processing of Cassandra data
 */
public class SparkProcessor implements Serializable {
    static final long serialVersionUID = 100L;
    private static Logger LOG = LoggerFactory.getLogger(SparkProcessor.class);


    public void processFileData() {
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
//         ************************ *************************************************************************************

        JavaRDD<String> cleansedCallTypes = callData.map( x -> (
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

        LOG.info("\n\n==================================CLEANSED AND SORTED BY FREQUENCY=============================\n");

        ListIterator<Tuple2<String, Integer>> cleansedListIterator = cleansedList.listIterator(cleansedList.size());
        while(cleansedListIterator.hasPrevious()){
            Tuple2<String, Integer> r = cleansedListIterator.previous();
            LOG.info("(" + r._1 + ", " + r._2 + ")");
        } // should combine some entries that were duplicates
    }

}
