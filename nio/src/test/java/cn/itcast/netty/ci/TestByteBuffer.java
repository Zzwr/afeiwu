package cn.itcast.netty.ci;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
@Slf4j
public class TestByteBuffer {
    public static void main(String[] args) throws IOException {
        //FileChannel
        //1.输入流、输出流  RandomAccessFile

        try(FileChannel channel = new FileInputStream("D:\\java\\idea\\IntelliJ IDEA 2018.2.3\\spark\\nio\\data.txt").getChannel()){
            ByteBuffer buffer = ByteBuffer.allocate(3);
            while (true){
                //从channel读取数据，向buffer写入
                int len = channel.read(buffer);
                log.debug("读取到的字节：{}",len);
                if (len == -1){
                    break;
                }
                //切换为buff的读模式
                buffer.flip();
                while (buffer.hasRemaining()){
                    //读取1字节
                    byte b = buffer.get();
                    log.debug("实际字节：{}",(char)b);
                }
                //切换为写模式
                buffer.clear();
            }
        }catch (Exception e){

        }
    }
}
