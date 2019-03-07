package groupOrder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupOrderReduce extends Reducer<OrderEntity, NullWritable, OrderEntity, NullWritable> {


    @Override
    protected void reduce(OrderEntity key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

