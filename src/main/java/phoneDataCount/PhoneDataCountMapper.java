package phoneDataCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneDataCountMapper extends Mapper<LongWritable, Text, Text, PhoneDataEntity> {

    private String phoneNumber;
    private PhoneDataEntity phoneDataEntity;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文本字段并拆分
        String lineValue = value.toString();
        String[] values = lineValue.split("\t");
        phoneNumber = values[1];
        long upFlow = Long.valueOf(values[values.length - 3]);
        long downFlow =Long.valueOf(values[values.length - 2]);
        context.write(new Text(phoneNumber), new PhoneDataEntity(upFlow,downFlow));
    }
}
