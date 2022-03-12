import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class UploadToHdfs {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),conf,"atguigu");
//        fs.copyFromLocalFile(new Path("C:\\Users\\Win\\Desktop\\复习\\spark优化\\资料\\1.数据文件"),new Path("/spark_optimization"));
//        fs.close();
        method1();
    }
    static void method1(){
        int a=Integer.MAX_VALUE;
        List l = new ArrayList<Integer>();
        while (true){
            l.add(a);
        }
    }

}
