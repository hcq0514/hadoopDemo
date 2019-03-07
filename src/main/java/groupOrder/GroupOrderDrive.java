package groupOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//求每个订单中最贵的商品（GroupingComparator）
public class GroupOrderDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job对象信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar位置
        job.setJarByClass(GroupOrderDrive.class);

        // 3 设置mapper和reducer的class类
        job.setMapperClass(GroupOrderMapper.class);
        job.setReducerClass(GroupOrderReduce.class);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(OrderEntity.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(OrderEntity.class);
        job.setOutputValueClass(NullWritable.class);

        // 处理小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 添加分区
        job.setPartitionerClass(GroupOrderPartitioner.class);
        job.setNumReduceTasks(3);

        // 9 关联Combiner
//		job.setCombinerClass(WordCountCombiner.class);

        //关联Comparator
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
