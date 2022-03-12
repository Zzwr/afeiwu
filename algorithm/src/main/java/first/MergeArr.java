package first;

import java.util.Arrays;

public class MergeArr {
    public static void main(String[] args) {
        int[] ints = merge2(new int[]{1,2,3,0,0,0}, 3, new int[]{2,5,6}, 3);
//        Arrays.asList(ints).forEach(System.out::println);
        for (int i = 0;i<ints.length;i++){
            System.out.println(ints[i]);
        }
    }
    static int[] merge2(int[] nums1, int m, int[] nums2, int n){
        /*
        *   p0 = m+n-1
        *   p1 = m+n-2
        *   pn = m+n-1-n
        * */
        int n1 = m-1;
        int n2 = n-1;
        int i = 1;
        while (n1>=0||n2>=0){
            if (n1<0){
                nums1[m+n-i] = nums2[n2--];
            }
            else if (n2<0){
                nums1[m+n-i] = nums1[n1--];
            }
            else if (nums1[n1]>nums2[n2]){
                nums1[m+n-i] = nums1[n1--];
            }else{
                nums1[m+n-i] = nums2[n2--];
            }
            i++;
        }
        return nums1;
    }
    //归并排序貌似比循环更好把
    static int[] merge1(int[] nums1, int m, int[] nums2, int n) {
        int l =m+n;
        int[] sort = new int[m + n];
        int n1 = 0;
        int n2 = 0;
        for (int i = 0; i < l; i++) {
            if (n2 == n) {
                sort[i] = nums1[n1++];
            } else if (n1 == m) {
                sort[i] = nums2[n2++];
            }
            else {
                sort[i] = nums1[n1] <= nums2[n2] ? nums1[n1++] : nums2[n2++];
            }
        }
        for (int i = 0;i!=m+n;++i){
            nums1[i]=sort[i];
        }
        return nums1;
    }
}
