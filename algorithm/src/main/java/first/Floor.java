package first;

public class Floor {
    public static void main(String[] args) {
        //System.out.println(step(5, 0));
        System.out.println(dp_step2(5));
    }
    //叶子n   节点n-1    时间：aF(n)+bF(n-1) = (a+b)F(n)-b = T(n) ====>所以时间复杂度T(n)=seita F(n) ---斐波那契通项公式
    static int step(int floor,int nowStep){
        int step1 = 1;
        int step2 = 2;
        if (nowStep<=floor-2){
            return step(floor,nowStep+2)+step(floor,nowStep+1);
        }
        if (nowStep<=floor-1){
            return step(floor,nowStep+1);
        }
        if (nowStep==floor){
            return 1;
        }
        return 0;
    }
    static int dp_step(int floor){
        int[] dp = new int[floor+2];
        dp[0] = 0;
        dp[1] = 1;
        dp[2] = 2;
        int n = 3;
        while (n<=floor){
            dp[n] = dp[n-1]+dp[n-2];
            n++;
        }
        return dp[floor];
    }
    static int dp_step2(int floor){
        int a1 = 1;
        int a2 = 2;
        int sum = 0;
        int n = 3;
        if (floor==1) return 1;
        if (floor==2) return 2;
        while (n<=floor){
            sum = a1+a2;
            a1 = a2;
            a2 = sum;
            n++;
        }
        return sum;
    }
}
