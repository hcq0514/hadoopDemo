package phoneDataCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
//统计每一个手机号耗费的总上行流量、下行流量、总流量

public class PhoneDataCountDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job对象信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar位置
        job.setJarByClass(PhoneDataCountDrive.class);

        // 3 设置mapper和reducer的class类
        job.setMapperClass(PhoneDataCountMapper.class);
        job.setReducerClass(PhoneDataCountReduce.class);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneDataEntity.class);

        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataEntity.class);

        // 处理小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 添加分区
//        job.setPartitionerClass(WordCountPartitioner.class);
//        job.setNumReduceTasks(2);

        // 9 关联Combiner
//		job.setCombinerClass(WordCountCombiner.class);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
