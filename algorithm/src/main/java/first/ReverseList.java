package first;

/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode() {}
 *     ListNode(int val) { this.val = val; }
 *     ListNode(int val, ListNode next) { this.val = val; this.next = next; }
 * }
 */
//链表可以选用迭代或递归方式完成反转
public class ReverseList {
    public ListNode reverseList(ListNode head) {
        if (head==null||head.next==null) return head;
        ListNode newHead = reverseList(head.next);
        head.next.next = head;
        head.next=null;
        return newHead;
    }
}
