import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
//
public class UnSafeDemo {
    static int _1GB = 1024*1024*1024;
    public static void main(String[] args) throws IOException {
        Unsafe unsafe = getUnsafe();
        //分配内存
        long base = unsafe.allocateMemory(_1GB);
        System.out.println(base);
        unsafe.setMemory(base,_1GB,(byte)0);
        System.in.read();
        //释放内存
        unsafe.freeMemory(base);
        System.in.read();
    }
    public static Unsafe getUnsafe(){
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            //private需要setAccessible
            f.setAccessible(true);
//            字段不是静态字段的话,要传入反射类的对象.如果传null是会报
//            java.lang.NullPointerException
//            但是如果字段是静态字段的话,传入任何对象都是可以的,包括null
            Unsafe unsafe = (Unsafe) f.get(null);
            return unsafe;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
