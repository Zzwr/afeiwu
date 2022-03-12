package com.atguigu.wc;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Int;

import java.util.Properties;

public class KafkaInAndOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("first", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapStream = source.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> map(String data) throws Exception {
                return new Tuple2<Integer,Integer>(Integer.parseInt(data),1);
            }
        });
        SingleOutputStreamOperator<Integer> apply = mapStream.keyBy(1).countWindow(10, 2).apply(new WindowFunction<Tuple2<Integer, Integer>, Integer, Tuple, GlobalWindow>() {
            public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<Integer> out) throws Exception {
                int sum = 0;
                System.out.println(sum+"--------------");
                System.out.println(IteratorUtils.toList(input.iterator()).size()+"000000000000000000000000");
                for (Tuple2<Integer, Integer> t : input) {
                    sum += t.f0;
                }
                out.collect(sum);
            }
        });
        apply.print();
        env.execute();
    }
}
