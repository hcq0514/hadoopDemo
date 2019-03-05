package phoneDataCount;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PhoneDataEntity implements Writable {

    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    // 反序列化时，需要反射调用空参构造函数，所以必须有
    public PhoneDataEntity() {
        super();
    }

    public PhoneDataEntity(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    //序列化写出
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //反序列化写入
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    //hadoop输出格式
    @Override
    public String toString() {
        return upFlow + "-----------" + downFlow + "-----------" + sumFlow;
    }



    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }



}
