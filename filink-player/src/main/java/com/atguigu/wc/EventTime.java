package com.atguigu.wc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
//import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Properties;

public class EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(5000);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("first", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Tuple2<String, Long>> mapSource = source.map(new MapFunction<String, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(String value) throws Exception {
                return new Tuple2<String, Long>("a", new Long(value));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> resStream = mapSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        });
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late"){};
        SingleOutputStreamOperator<Tuple2<String, Long>> apply = resStream.keyBy(0)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(15))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                for (Tuple2<String, Long> tuple2 : input) {
                    out.collect(tuple2);
                }
            }
        });
        apply.print();
        env.execute();
    }
}
