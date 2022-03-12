import java.nio.ByteBuffer;

public class Demo1 {
    public static void main(String[] args) {
        String s1 = "a";
        String s2 = "b";
        String s4 = s1+s2;
        String s6 = s4.intern();
        String s5 = "a"+"b";
        String s3 = "ab";
        System.out.println(s3=="ab");
        System.out.println(s3==s4);
        System.out.println(s4=="ab");
        System.out.println(s3==s5);
    }
}
