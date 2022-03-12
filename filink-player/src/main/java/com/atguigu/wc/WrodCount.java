package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.util.Collector;


//批处理wordcount
public class WrodCount {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        Thread.sleep(3000);
        System.out.println(System.currentTimeMillis());
    }
}
