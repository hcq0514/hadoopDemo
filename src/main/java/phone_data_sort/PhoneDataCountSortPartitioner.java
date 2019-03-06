package phone_data_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import phone_data.PhoneDataEntity;

public class PhoneDataCountSortPartitioner extends Partitioner<PhoneDataSortEntity, Text> {


    //判断它处于哪个分片中
    @Override
    public int getPartition(PhoneDataSortEntity phoneDataSortEntity, Text key, int numPartitions) {
        // 1 获取电话号码的前三位
        String preNum = key.toString().substring(0, 3);
        int partition = 4;

        // 2 判断是哪个省
        if ("136".equals(preNum)) {
            partition = 0;
        } else if ("137".equals(preNum)) {
            partition = 1;
        } else if ("138".equals(preNum)) {
            partition = 2;
        } else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;
    }

}
