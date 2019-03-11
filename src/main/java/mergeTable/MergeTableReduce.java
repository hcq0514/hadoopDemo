package mergeTable;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class MergeTableReduce extends Reducer<LongWritable, ProductEntity, ProductEntity, NullWritable> {

    static String productName = "";

    @Override
    protected void reduce(LongWritable key, Iterable<ProductEntity> values, Context context) {
        //用于存储不带productName的数据
        ArrayList<ProductEntity> productEnties = new ArrayList<ProductEntity>();
        for (ProductEntity v : values) {
            if (v.isProductCompany()) {
                //获取厂商名称
                productName = v.getpName();
            } else {
                ProductEntity productEntity = new ProductEntity();
                try {
                    BeanUtils.copyProperties(productEntity, v);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                productEnties.add(productEntity);
            }
        }
        //设置厂商名称并输出
        productEnties.forEach(productEntity -> {
            try {
                productEntity.setpName(productName);
                context.write(productEntity, NullWritable.get());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}

