package groupOrder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupOrderMapper extends Mapper<LongWritable, Text, OrderEntity, NullWritable> {

    private OrderEntity orderEntity = new OrderEntity();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文本字段并拆分
        String lineValue = value.toString();
        String[] values = lineValue.split("\t");
        orderEntity.setOrderId(values[0]);
        orderEntity.setPrice(Double.valueOf(values[2]));
        context.write(orderEntity, NullWritable.get());
    }
}
