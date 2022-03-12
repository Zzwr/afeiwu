package review;

public class MoveZero {
    public static void main(String[] args) {
        moveZeroes(new int[]{0,1,0,3,12});
    }
    public static void moveZeroes(int[] nums) {
        int left=0;
        int len = nums.length;
        for (int right=0;right < len;right++){
            if (nums[right]!=0){
                int temp = nums[left];
                nums[left] = nums[right];
                nums[right] = temp;
                left++;
            }

        }
    }
}
