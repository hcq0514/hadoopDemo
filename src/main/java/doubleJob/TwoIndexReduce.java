package doubleJob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReduce extends Reducer<Text, Text, Text, Text> {

//    String v = ""; 变量不能放这里，会导致各个reduce都一直在用这个值，后面会把前面的reduce产生的数据全部叠加

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String v = "";
        for (Text value : values) {
            v = v + value+"  ";
        }

        // 2输出所有单词个数
        context.write(key, new Text(v));
    }
}

