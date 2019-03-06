package SortByASCParity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortByASCParityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文本字段并拆分
        String lineValue = value.toString();
        String[] values = lineValue.split(" ");
        //写入context
        for (String v:values) {
            context.write(new Text(v),new IntWritable(1));
        }

    }
}
