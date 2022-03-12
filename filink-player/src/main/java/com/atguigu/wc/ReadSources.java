package com.atguigu.wc;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

//id:int
//time:long
//wd:long
class TestBean{
    private Integer id;
    private Long time;
    private Long wd;
    TestBean(Integer id,Long time,Long wd){
        this.id = id;
        this.time = time;
        this.wd = wd;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getWd() {
        return wd;
    }

    public void setWd(Long wd) {
        this.wd = wd;
    }

    @Override
    public String toString() {
        return "TestBean{" +
                "id='" + id + '\'' +
                ", time='" + time + '\'' +
                ", wd=" + wd +
                '}';
    }
}
public class ReadSources {
    //数组中读取数据
    public static void read1(StreamExecutionEnvironment env){
        DataStreamSource<TestBean> datasource = env.fromCollection(Arrays.asList(
                new TestBean(1, System.currentTimeMillis(), 3L),
                new TestBean(2, System.currentTimeMillis(), 3L),
                new TestBean(3, System.currentTimeMillis(), 3L),
                new TestBean(4, System.currentTimeMillis(), 3L)
        ));
        datasource.print();
    }
    //文件中读取
    public static void read2(StreamExecutionEnvironment env){
        DataStreamSource<String> read = env.readTextFile("D:\\java\\idea\\IntelliJ IDEA 2018.2.3\\spark\\filink-player\\src\\main\\resources\\hello.txt");
        read.print();
    }
    //自定义数据源
    public static void read3(StreamExecutionEnvironment env){
        DataStreamSource<TestBean> mySource = env.addSource(new SourceFunction<TestBean>() {
            Boolean flg = true;

            public void run(SourceContext<TestBean> ctx) throws Exception {
                while (flg) {
                    Random rdm = new Random();
                    ctx.collect(new TestBean(rdm.nextInt(10), rdm.nextLong(), rdm.nextLong()));
                }
            }

            public void cancel() {
                flg = false;
            }
        });
        mySource.print();
    }
    //kafka读取数据源
    public static void read4(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("first", new SimpleStringSchema(), properties));
        source.print();
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //read1(env);
        //read2(env);
        //read3(env);
        read4(env);
        env.execute();
    }
}
