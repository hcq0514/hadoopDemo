package groupOrder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupOrderPartitioner extends Partitioner<OrderEntity, NullWritable> {


    //判断它处于哪个分片中
    @Override
    public int getPartition(OrderEntity phoneDataSortEntity, NullWritable nullWritable, int numPartitions) {
        return (phoneDataSortEntity.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}
