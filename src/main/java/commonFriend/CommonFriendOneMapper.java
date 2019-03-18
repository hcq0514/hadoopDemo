package commonFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendOneMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text v ;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(":");
        v = new Text(s[0]);
        String[] followedUser = s[1].split(",");
        for (String t : followedUser) {
            context.write(new Text(t), v);
        }
    }

}
