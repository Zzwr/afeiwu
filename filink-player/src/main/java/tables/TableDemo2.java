package tables;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

public class TableDemo2 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
//        TableEnvironment tEnv = TableEnvironment.create(settings);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv
                .connect(new FileSystem().path("D:\\java\\venv\\tt.csv"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("name",DataTypes.STRING()))
                .createTemporaryTable("firstTable");
        Table firstTable = tEnv.from("firstTable");
//        Table select = firstTable.select("id,name");

        tEnv
                .connect(new FileSystem().path("D:\\java\\venv\\output.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                    .field("id",DataTypes.STRING())
                    .field("name", DataTypes.STRING()))
                .createTemporaryTable("outputTable");
        firstTable.insertInto("outputTable");
        tEnv.execute("aaa");
    }
}
