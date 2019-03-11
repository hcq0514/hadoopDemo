package mergeTable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class ProductEntity implements Writable {

    private Long id = -1L;
    private Long pid = -1L;
    private Double amount = -1.0;
    private String pName = "";
    private boolean productCompany ;




    // 反序列化时，需要反射调用空参构造函数，所以必须有
    public ProductEntity() {
        super();
    }

    public ProductEntity(Long id, Long pid, Double amount, String pName, Boolean productCompany) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pName = pName;
        this.productCompany = productCompany;
    }

    //序列化写出
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(pid);
        out.writeDouble(amount);
        out.writeUTF(pName);
        out.writeBoolean(productCompany);
    }


    //反序列化写入
    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        pid = in.readLong();
        amount = in.readDouble();
        pName = in.readUTF();
        productCompany = in.readBoolean();
    }

    //hadoop输出格式
    @Override
    public String toString() {
        return id + "-----------" + pName + "-----------" + amount;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public boolean isProductCompany() {
        return productCompany;
    }

    public void setProductCompany(boolean productCompany) {
        this.productCompany = productCompany;
    }
}
