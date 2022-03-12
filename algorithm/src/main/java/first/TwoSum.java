package first;

import java.util.HashMap;

public class TwoSum {
    public static void main(String[] args) {

    }
    public int[] twoSum(int[] nums,int target){
        HashMap<Integer,Integer> map = new HashMap();
        for (int i = 0;i<nums.length;i++){
            if (map.containsKey(target-nums[i])){
                //先在外面创建数组也许会更好。
                return new int[]{map.get(target-nums[i]),i};
            }
            map.put(nums[i],i);
        }
        return null;
    }
}
