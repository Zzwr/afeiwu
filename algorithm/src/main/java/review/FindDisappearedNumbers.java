package review;

import java.util.ArrayList;
import java.util.List;

public class FindDisappearedNumbers {
    public List<Integer> findDisappearedNumbers(int[] nums) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0;i<nums.length;i++){
            int index = (nums[i] - 1)%nums.length;
            nums[index] = nums[index]+nums.length;
        }
        for (int i = 0;i<nums.length;i++){
            if (nums[i]>nums.length) list.add(i+1);
        }
        return list;
    }
}
