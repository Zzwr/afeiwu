import java.util.ArrayList;
import java.util.List;

public class RefDemo {
    private static final int _4mb = 4*1024*1024;
    private static final byte[] b = new byte[4*1024*1024] ;
    public static void main(String[] args) {
        List<byte[]> list = new ArrayList<byte[]>();
        for (int i = 0;i<9;i++) {
            System.out.println(i);
            list.add(b);
        }
    }
}
