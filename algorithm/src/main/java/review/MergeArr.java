package review;

public class MergeArr {
    public void merge(int[] nums1, int m, int[] nums2, int n) {
        int i1 = m-1;
        int i2 = n-1;
        int i = 0;
        while (i1<m||i2<n){
            if (i1<0){
                nums1[m+n-1-i] = nums2[i2--];
            }
            else if (i2<0){
                nums1[m+n-1-i] = nums1[i1--];
            }
            else if (nums1[i1]<=nums2[i2]){
                nums1[m+n-1-i] = nums2[i2--];
            }
            else {
                nums1[m+n-1-i] = nums1[i1--];
            }
            i++;
        }
    }
}
