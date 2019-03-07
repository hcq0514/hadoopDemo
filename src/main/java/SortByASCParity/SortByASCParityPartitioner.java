package SortByASCParity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortByASCParityPartitioner extends Partitioner<Text, IntWritable> {


    //判断它处于哪个分片中
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        char c = key.toString().charAt(0);
        if (c % 2 != 0) {
            numPartitions = 0;
        } else {
            numPartitions = 1;
        }
        return numPartitions;
    }

}
