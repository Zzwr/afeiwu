package first;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Definition for singly-linked list.
 * class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) {
 *         val = x;
 *         next = null;
 *     }
 * }
 */
//数学题啊  跳过把  直接hashmap得了
public class HasCycleTwo {
    public ListNode detectCycle(ListNode head) {
        HashSet map = new HashSet();
        while (head!=null){
            if (map.contains(head.val)){
                return head;
            }
            else {
                map.add(head.val);
            }
            head=head.next;
        }
        return null;
    }
}
