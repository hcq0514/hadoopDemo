package commonFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFriendOneReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb = sb.append(value).append(",");
        }
        //切掉最后的,
        context.write(key, new Text(sb.substring(0, sb.length() - 1)));
    }
}

