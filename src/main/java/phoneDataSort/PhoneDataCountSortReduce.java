package phoneDataSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneDataCountSortReduce extends Reducer<PhoneDataSortEntity, Text, Text, PhoneDataSortEntity> {



    protected void reduce(Iterable<PhoneDataSortEntity> values, Text key, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        //相同的key会进入到同一个reduce里面 所有这里面循环的也是同一个key的所有值
        for (PhoneDataSortEntity entity : values) {
            upFlow = upFlow + entity.getUpFlow();
            downFlow = downFlow + entity.getDownFlow();
        }
        // 2输出
        context.write(key, new PhoneDataSortEntity(upFlow, downFlow));
    }
}

