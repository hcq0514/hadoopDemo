package groupOrder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {

    //必须创建，否则无法初始化，会报错
    protected OrderGroupComparator() {
        super(OrderEntity.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderEntity o1 = (OrderEntity) a;
        OrderEntity o2 = (OrderEntity) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
