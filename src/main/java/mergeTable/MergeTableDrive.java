package mergeTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

//在reduce端进行多表关联
public class MergeTableDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        // 1 获取job对象信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar位置
        job.setJarByClass(MergeTableDrive.class);

        // 3 设置mapper和reducer的class类
        job.setMapperClass(MergeTableMapper.class);
        job.setReducerClass(MergeTableReduce.class);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ProductEntity.class);

        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(ProductEntity.class);
        job.setOutputValueClass(NullWritable.class);


        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // 9 关联Combiner
//		job.setCombinerClass(WordCountCombiner.class);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
