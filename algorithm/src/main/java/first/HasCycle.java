package first;

import java.util.HashMap;
import java.util.Map;

class ListNode1 {
    int val;
    ListNode next;
    ListNode1(int x) {
        val = x;
        next = null;
    }
}
public class HasCycle {
    public boolean hasCycle(ListNode head) {
        if (head==null||head.next==null) return false;
        ListNode slow = head;
        ListNode fast = head.next;
        while (slow!=fast){
            if (fast==null||fast.next==null) return false;
            fast = fast.next.next;
            slow = slow.next;
        }
        return true;
    }
}
