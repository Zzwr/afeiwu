package first;
class ListNode {
    int val;
    ListNode next;
    ListNode() {}
    ListNode(int val) { this.val = val; }
    ListNode(int val, ListNode next) { this.val = val; this.next = next; }
}
public class DeleteDuplicates {
    public ListNode deleteDuplicates(ListNode head) {
        ListNode thead = head;
        while (thead!=null&&thead.next!=null){
            if (thead.val==thead.next.val){
                thead.next = thead.next.next;
            }
            else {
                thead = thead.next;
            }
        }
        return head;
        //        if (head.next==null) return head;
//        else if (head.val==head.next.val){
//            head.next = deleteDuplicates(head.next);
//            return head;
//        }
//        else {
//            return deleteDuplicates(head.next);
//        }
    }
}
