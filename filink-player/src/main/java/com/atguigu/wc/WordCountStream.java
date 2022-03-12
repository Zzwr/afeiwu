package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\java\\idea\\IntelliJ IDEA 2018.2.3\\spark\\filink-player\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> input = env.readTextFile(path);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(new Tuple2<String, Integer>(split, 1));
                }
            }
        }).keyBy(0).sum(1);
        result.print();
        env.execute();
    }
}
