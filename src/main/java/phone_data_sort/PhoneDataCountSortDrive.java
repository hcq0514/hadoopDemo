package phone_data_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
//统计每一个手机号耗费的总上行流量、下行流量、总流量,并且根据手机号归属地分片，并按总流量排序4

public class PhoneDataCountSortDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job对象信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar位置
        job.setJarByClass(PhoneDataCountSortDrive.class);

        // 3 设置mapper和reducer的class类
        job.setMapperClass(PhoneDataCountSortMapper.class);
        job.setReducerClass(PhoneDataCountSortReduce.class);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(PhoneDataSortEntity.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataSortEntity.class);

        // 处理小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 添加分区
        job.setPartitionerClass(PhoneDataCountSortPartitioner.class);
        //创建的分区数量，默认从分区0-1-2上这样创建
        job.setNumReduceTasks(5);

        // 9 关联Combiner
//		job.setCombinerClass(WordCountCombiner.class);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
