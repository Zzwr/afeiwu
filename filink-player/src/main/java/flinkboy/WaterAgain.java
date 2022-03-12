package flinkboy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

public class WaterAgain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> input = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, new Random().nextInt(5));
            }
        });
        SingleOutputStreamOperator<Tuple3<String, Long, Tuple2<String, Integer>>> reduce = input.keyBy(0)
//                .timeWindow(Time.seconds(2))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Tuple2<String, Integer>>, Tuple, TimeWindow>() {
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Long, Tuple2<String, Integer>>> out) throws Exception {
                        Tuple2<String, Integer> next = elements.iterator().next();
                        out.collect(Tuple3.of(tuple.getField(0).toString(), 1L, next));
                    }
                });
        reduce.print();
        env.execute();
    }
}
