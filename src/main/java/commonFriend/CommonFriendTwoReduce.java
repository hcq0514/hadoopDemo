package commonFriend;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFriendTwoReduce extends Reducer<Text, Text, Text, Text> {
    static List<String> keys = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //如果已存在重复key，则不输出
        if (isReapt(key.toString())) {
            return;
        }

        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb = sb.append(value).append(",");
        }
        keys.add(key.toString());
        //切掉最后的逗号
        context.write(key, new Text(sb.substring(0, sb.length() - 1)));
    }

    /**
     * 判断是否已存在两个组合的k
     *
     * @param k
     * @return
     */
    private boolean isReapt(String k) {
        if (keys.contains(k)||keys.contains(StringUtils.reverse(k))) {
            return true;
        }
        return false;
    }
}

