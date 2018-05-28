package com.liss.hadoop.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.liss.hadoop.utils.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * 在某些情况下，SemiJoin抽取出来的小表的key集合在内存中仍然存放不下，这时候可以使用BloomFiler以节省空间，一般是 Reduce Side Join 和 BloomFilter 一起使用
 * <p>
 * BloomFilter最常见的作用是：判断某个元素是否在一个集合里面。它最重要的两个方法是：add() 和membershipTest ()。
 * <p>
 * 因而可将小表中的key保存到BloomFilter中，在map阶段过滤大表，可能有一些不在小表中的记录没有过滤掉（但是在小表中的记录一定不会过滤掉），这没关系，只不过增加了少量的网络IO而已。
 * <p>
 * 一个大表，一个小表
 * map 阶段：BloomFilter 解决小表的key集合在内存中仍然存放不下的场景，过滤大表
 * reduce 阶段：reduce side join
 */
public class BloomFilterJoin {
    /**
     * 为来自不同表(文件)的key/value对打标签以区别不同来源的记录。
     * 然后用连接字段作为key，其余部分和新加的标志作为value，最后进行输出。
     */
    public static class BloomFilteringMapper extends
            Mapper<Object, Text, Text, Text> {
        // 第一个参数是vector的大小，这个值尽量给的大，可以避免hash对象的时候出现索引重复
        // 第二个参数是散列函数的个数
        // 第三个是hash的类型，虽然是int型，但是只有默认两个值
        // 哈希函数个数k、位数组大小m及字符串数量n之间存在相互关系
        //n 为小表记录数,给定允许的错误率E，可以确定合适的位数组大小，即m >= log2(e) * (n * log2(1/E))
        // 给定m和n，可以确定最优hash个数，即k = ln2 * (m/n)，此时错误率最小
        private BloomFilter filter = new BloomFilter(10000, 6, Hash.MURMUR_HASH);
        private Text joinKey = new Text();
        private Text combineValue = new Text();

        /**
         * 获取分布式缓存文件
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            //获得当前作业的DistributedCache相关文件
            URI[] uris = context.getCacheFiles();
            String joinKeyStr = null;
            for (URI uri : uris) {
                Path path = new Path(uri.getPath());
                if (path.toString().endsWith("join_key.txt")) {
                    //读缓存文件，并放到mem中
                    br = new BufferedReader(new FileReader(path.toString()));
                    while (null != (joinKeyStr = br.readLine())) {
                        filter.add(new Key(joinKeyStr.getBytes()));
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String pathName = ((FileSplit) context.getInputSplit()).getPath()
                    .toString();
            // 如果数据来自于records，加一个records的标记
            if (pathName.endsWith("left_table.txt")) {
                String[] valueItems = value.toString().split("\\s+");
                // 过滤掉脏数据
                if (valueItems.length != 5) {
                    return;
                }
                //通过filter 过滤大表中的数据
                if (filter.membershipTest(new Key(valueItems[0].getBytes()))) {
                    joinKey.set(valueItems[0]);
                    combineValue.set("0" + valueItems[1] + "\t" + valueItems[2]);
                    context.write(joinKey, combineValue);
                }

            } else if (pathName.endsWith("right_table.txt")) {
                // 如果数据来自于station，加一个station的标记
                String[] valueItems = value.toString().split("\\s+");
                // 过滤掉脏数据
                if (valueItems.length != 4) {
                    return;
                }
                joinKey.set(valueItems[3]);
                combineValue.set("1" + valueItems[1] +"\t" + valueItems[2]);
                context.write(joinKey, combineValue);
            }

        }
    }

    /*
     * reduce 端做笛卡尔积
     */
    public static class BloomFilteringReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftTable = new ArrayList<String>();
        private List<String> rightTable = new ArrayList<String>();
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 一定要清空数据
            leftTable.clear();
            rightTable.clear();
            // 相同key的记录会分组到一起，我们需要把相同key下来自于不同表的数据分开，然后做笛卡尔积
            for (Text value : values) {
                String val = value.toString();
                System.out.println("key : "+key +", value : " + val);
                if (val.startsWith("0")) {
                    leftTable.add(val.replaceFirst("0", ""));
                } else if (val.startsWith("1")) {
                    rightTable.add(val.replaceFirst("1", ""));
                }
            }
            // 笛卡尔积
            for (String leftPart : leftTable) {
                for (String rightPart : rightTable) {
                    result.set(leftPart + "\t" + rightPart);
                    context.write(key, result);
                }
            }
        }
    }

    private static final String INPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/join/*table.txt";
    // 假设 key 已经过滤到 join_key.txt文件
    private static final String CACHE_INPUT_PAHT = "D:/GitWorkspace/hadoop_test/src/main/resources/join/join_key.txt";
    private static final String OUTPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/join/stats_bloom_filter_join";

    public static void main(String[] arg0) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "BloomFilterJoin");

        FileUtils.deleteDirectory(OUTPUT_PATH, conf);

        //添加缓存文件
        job.addCacheFile(new Path(CACHE_INPUT_PAHT).toUri());

        job.setJarByClass(BloomFilterJoin.class);

        job.setMapperClass(BloomFilteringMapper.class);
        job.setReducerClass(BloomFilteringReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
