package com.atguigu.wc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class ProcessFunc2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        final OutputTag<Senor> low = new OutputTag<Senor>("ww"){};
        SingleOutputStreamOperator<Senor> mapSource = source.map(new MapFunction<String, Senor>() {
            public Senor map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Senor(splits[0], Double.parseDouble(splits[1]), new Long(splits[2]));
            }
        });
        SingleOutputStreamOperator<Senor> high = mapSource.process(new ProcessFunction<Senor, Senor>() {
            public void processElement(Senor value, Context ctx, Collector<Senor> out) throws Exception {
                if (value.getTemp() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(low, value);
                }
            }
        });

        high.print("high");
        high.getSideOutput(low).print("low");
        env.execute();
    }
}
