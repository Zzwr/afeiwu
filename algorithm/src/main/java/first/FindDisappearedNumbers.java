package first;

import java.util.ArrayList;
import java.util.List;

public class FindDisappearedNumbers {
    public static void main(String[] args) {
        findDisappearedNumbers2(new int[]{4,3,2,7,8,2,3,1}).forEach(System.out::println);
    }
    //原地修改
    public static List<Integer> findDisappearedNumbers(int[] nums) {
        ArrayList<Integer> integers = new ArrayList<>();
        for (int i = 0;i<nums.length;i++){
            int x = (nums[i]-1)%nums.length;
            nums[x] = nums[x]+nums.length;
        }
        for (int i = 0;i<nums.length;i++){
            if (nums[i]<=nums.length) integers.add(i+1);
        }
        return integers;
    }
    //原地哈希
    public static List<Integer> findDisappearedNumbers2(int[] nums) {
        int len = nums.length;
        for (int i=0;i<len;i++){
            if (nums[i]==i+1) continue;
            if (nums[i] == nums[nums[i]-1]){
                continue;
            }
            int temp = nums[i];
            nums[i] = nums[nums[i]-1];
            nums[temp-1] = temp;
            i--;
        }
        ArrayList<Integer> objects = new ArrayList<>();
        for (int i=0;i<nums.length;i++){
            if (nums[i]!=i+1){
                objects.add(i+1);
            }
        }
        return objects;
    }
}
