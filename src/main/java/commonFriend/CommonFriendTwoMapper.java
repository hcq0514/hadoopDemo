package commonFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFriendTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text v;
    static List<String> keys = new ArrayList<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split("\t");
        v = new Text(s[0]);
        String[] followUser = s[1].split(",");
        //循环遍历，以两人一组为k
        String k;

        for (int i = 0; i < followUser.length; i++) {
            String user = followUser[i];
            for (int j = 0; j < followUser.length; j++) {
                if (!user.equals(followUser[j])) {
                    k = user + followUser[j];
                    context.write(new Text(k), v);
                }
            }
        }
    }



}
