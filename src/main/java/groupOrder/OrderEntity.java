package groupOrder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderEntity implements WritableComparable<OrderEntity> {
    private String orderId;
    private Double price;

    public OrderEntity() {
        super();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        price = in.readDouble();
    }


    //compareTo方法，根据result对比的值，返回的是正数则是大于。对比后小的值排在前面，大的排在后面
    public int compareTo(OrderEntity o) {
        int result;
        if (this.getOrderId().compareTo(o.getOrderId()) > 0) {
            result = 1;
        } else if (this.getOrderId().compareTo(o.getOrderId()) < 0) {
            result = -1;
        } else {
            if (this.getPrice() < o.getPrice()) {
                result = 1;
            } else if (this.getPrice() > o.getPrice()) {
                result = -1;
            } else {
                result = 0;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return orderId + "----------------------" + price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
