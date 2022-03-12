import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseConfTool;

import java.io.IOException;
import java.util.Arrays;

public class HBaseConnect {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("plmm", "mymm"));
        Put put = new Put(Bytes.toBytes("1001"));
        put.addColumn(Bytes.toBytes("mm_info"),Bytes.toBytes("baisi"),Bytes.toBytes("yes"));
        table.put(put);
        conn.close();
    }
}
