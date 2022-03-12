package review;

public class Floor {
    public static void main(String[] args) {
    }
    public int climbStairs(int n) {
        int s1 = 1;
        int s2 = 1;
        int now = 1;
        for (int s = 2;s<n;s++){
            now = s1+s2;
            s1 = s2;
            s2 = now;
        }
        return now;
    }
}
