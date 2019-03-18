package commonFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 思路：
 * 首先找到每个人的粉丝(关注他的人) 输出成以下模式：A	I,K,C,B,G,F,H,O,D
 * 将关注他的人 两两组合作为key 把被关注的人作为value 在reduce中叠加 输出：AF	B,C,D,O,E
 */
public class CommonFriendOneDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        // 1 获取job对象信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar位置
        job.setJarByClass(CommonFriendOneDrive.class);

        // 3 设置mapper和reducer的class类
        job.setMapperClass(CommonFriendOneMapper.class);
        job.setReducerClass(CommonFriendOneReduce.class);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}