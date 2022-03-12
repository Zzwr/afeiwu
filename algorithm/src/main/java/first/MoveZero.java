package first;

public class MoveZero {
    public static void main(String[] args) {
        moveZeroes(new int[]{0,1,0,3,12});
    }
    //count：记录下来当前遇到0的个数
    //index：当前元素在
    public static void moveZeroes(int[] nums) {
        int count = 0;
        for (int i = 0;i<nums.length;i++){
            if (nums[i]==0) count++;
            else nums[i-count] = nums[i];
        }
        for (int i = nums.length-count;i<nums.length;i++){
            nums[i]=0;
        }
    }
}
