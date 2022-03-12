package com.atguigu.wc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ProcessFunc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("first", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Senor> objectSource = source.map(new MapFunction<String, Senor>() {
            public Senor map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Senor(splits[0], Double.parseDouble(splits[1]), new Long(splits[2]));
            }
        });
        objectSource.keyBy("id").process(new MyKeyedProFunc()).print();
        env.execute();
    }
    public static class MyKeyedProFunc extends KeyedProcessFunction<Tuple,Senor,String>{
        private ValueState<Double> temp;
        private ValueState<Long> time;
        @Override
        public void open(Configuration parameters) throws Exception {
            temp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", Types.DOUBLE,0.0));
            time = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Types.LONG));
        }

        public void processElement(Senor value, Context ctx, Collector<String> out) throws Exception {
            long curProcessTime = ctx.timerService().currentProcessingTime();
            System.out.println(time.value()+"---"+temp.value());
            //todo:第一次初始
            if (temp.value()>temp.value()&&time.value()==null){
                Long ts = curProcessTime+10000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                time.update(ts);
            }
            //todo:第二次
            else if (value.getTemp()<temp.value()){
                System.out.println("小于");
                ctx.timerService().deleteProcessingTimeTimer(time.value());
                time.clear();
            }
            temp.update(value.getTemp());
            System.out.println((time.value()==null)+"-------"+(time.value())+"-----"+temp.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey().getField(0)+"温度连续上升...");
            time.clear();
        }

        @Override
        public void close() throws Exception {
            temp.clear();
        }
    }
}
