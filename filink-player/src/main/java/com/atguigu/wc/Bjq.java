package com.atguigu.wc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Bjq {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("first", new SimpleStringSchema(), properties));
        source.map(new MyMapFunc()).print();
        env.execute();
    }
    public static class MyMapFunc extends RichMapFunction<String, Tuple3<String,String,String>> implements ListCheckpointed<String>{
        private String last=null;

        public Tuple3<String,String,String> map(String value) throws Exception {
            if (last==null){
                last = value;
                return new Tuple3<String, String, String>(value,"null","nomal");
            }
            if (Integer.parseInt(value)-Integer.parseInt(last)>10){
                return new Tuple3<String, String, String>(value,last,"warning");
            }
            String a= last;
            last = value;
            return new Tuple3<String, String, String>(value,last,"nomal");
        }

        public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(last);
        }

        public void restoreState(List<String> state) throws Exception {
            for (String s:state) last = last + s;
        }
    }
}
