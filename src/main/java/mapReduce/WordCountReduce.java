package mapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        //相同的key会进入到同一个reduce里面 所有这里面循环的也是同一个key的所有值
        for(IntWritable value:values){
            count += value.get();
        }

        // 2输出所有单词个数
        context.write(key, new IntWritable(count));
    }
}

