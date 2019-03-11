package mergeTable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MergeTableMapper extends Mapper<LongWritable, Text, LongWritable, ProductEntity> {

    private String pid;
    private ProductEntity productEntity = new ProductEntity();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取输入文件类型
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        String lineValue = value.toString();
        String[] values = lineValue.split("\t");
        if (name.startsWith("product_company")) {
            productEntity.setPid(Long.valueOf(values[0]));
            productEntity.setpName(values[1]);
            productEntity.setProductCompany(true);
        } else {
            productEntity.setId(Long.valueOf(values[0]));
            productEntity.setPid(Long.valueOf(values[1]));
            productEntity.setAmount(Double.valueOf(values[2]));
            productEntity.setProductCompany(false);
        }

        context.write(new LongWritable(productEntity.getPid()), productEntity);
    }
}
