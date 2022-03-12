package flinkboy;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.util.Collector;

public class WaterAgain2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(value -> new Tuple2<String, Integer>(value, 1));
        KeyedStream<Tuple2<String, Integer>, Tuple> key = map.keyBy(0);
        key.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, String>() {
            MapStateDescriptor<String, Integer> ss;
            @Override
            public void open(Configuration parameters) throws Exception {

                 ss = new MapStateDescriptor<>("ss", Types.STRING, Types.INT);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value,
                                       Context ctx,
                                       Collector<String> out) throws Exception {
                TimerService ts = ctx.timerService();
                long processingTime = ts.currentProcessingTime();
                long tm = processingTime + 5000;
                ts.registerProcessingTimeTimer(tm);

            }
        });
        env.execute();
    }
}
