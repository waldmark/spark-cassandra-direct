package com.objectpartners.config;


import com.objectpartners.aws.S3Client;
import com.objectpartners.cassandra.CassandraDataLoader;
import com.objectpartners.common.components.Map911Call;
import com.objectpartners.common.domain.CallFrequency;
import com.objectpartners.common.domain.RealTime911;
import com.objectpartners.spark.SparkProcessor;
import com.objectpartners.spark.SparkCassandraRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.objectpartners"})
public class SparkCassandraConfig {

    @Bean
    SparkCassandraRunner sparkCassandraRunner() {
        return new SparkCassandraRunner();
    }

    @Bean
    SparkProcessor sparkProcessor() {
        return new SparkProcessor();
    }

    @Bean
    S3Client s3Client() {
        return new S3Client();
    }

    @Bean
    CassandraDataLoader cassandraDataLoader() {
        return new CassandraDataLoader();
    }

    @Bean
    Map911Call map911Call() {
        return new Map911Call();
    }

    @Bean
    CallFrequency callFrequency() {
        return new CallFrequency();
    }

    @Bean
    RealTime911 realTime911() {
        return new RealTime911();
    }

}
