package phone_data_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneDataCountSortMapper extends Mapper<LongWritable, Text, PhoneDataSortEntity, Text> {

    private String phoneNumber;
    private PhoneDataSortEntity phoneDataSortEntity;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文本字段并拆分
        String lineValue = value.toString();
        String[] values = lineValue.split("\t");
        phoneNumber = values[1];
        long upFlow = Long.valueOf(values[values.length - 3]);
        long downFlow = Long.valueOf(values[values.length - 2]);
        context.write(new PhoneDataSortEntity(upFlow, downFlow), new Text(phoneNumber));
    }
}
