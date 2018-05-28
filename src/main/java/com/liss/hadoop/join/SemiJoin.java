package com.liss.hadoop.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

import com.liss.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SemiJoin，也叫半连接，是从分布式数据库中借鉴过来的方法。它的产生动机是：对于reduce side join，跨机器的数据传输量非常大，
 * 这成了join操作的一个瓶颈，如果能够在map端过滤掉不会参加join操作的数据，则可以大大节省网络IO。
 *
 * 一般的使用场景，因为 小表 也无法放入内存中，而小表中用于关联的 Key 能够放入内存中
 *
 * 实现方法：
 * 需要两个MapReduce Job分两次完成
 * 1. 第一个 MapReduce 任务，选取一个小表，假设是File1，将其参与join的key抽取出来并去重，保存到文件File3中，File3文件一般很小，可以放到内存中
 * 2. 第二个 MapReduce 任务，在map阶段，使用DistributedCache将File3缓存到各个Node上，然后将File2中不在File3中的key对应的记录过滤掉，剩下的reduce阶段的工作与reducee side join相同。
 */
public class SemiJoin {
    private static final Logger logger = LoggerFactory.getLogger(SemiJoin.class);

    public static class SemiJoinMapper extends Mapper<Object, Text, Text, ReduceSideJoin.CombineValues> {
        private ReduceSideJoin.CombineValues combineValues = new ReduceSideJoin.CombineValues();
        private HashSet<String> joinKeySet = new HashSet<String>();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text secondPart = new Text();

        /**
         * 将参加join的key从DistributedCache取出放到内存中，以便在map端将要参加join的key过滤出来。b
         */
        @Override
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
                        joinKeySet.add(joinKeyStr);
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //获得文件输入路径
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            //数据来自left_table文件,标志即为"0"
            if (pathName.endsWith("left_table.txt")) {
                String[] valueItems = value.toString().split("\\s+");
                //过滤格式错误的记录
                if (valueItems.length != 5) {
                    return;
                }
                //过滤掉不需要参加join的记录
                if (joinKeySet.contains(valueItems[0])) {
                    flag.set("0");
                    joinKey.set(valueItems[0]);
                    secondPart.set(valueItems[1] + "\t" + valueItems[2] + "\t" + valueItems[3] + "\t" + valueItems[4]);
                    combineValues.setFlag(flag);
                    combineValues.setJoinKey(joinKey);
                    combineValues.setSecondPart(secondPart);
                    context.write(combineValues.getJoinKey(), combineValues);
                } else {
                    return;
                }
            }//数据来自于right_table，标志即为"1"
            else if (pathName.endsWith("right_table.txt")) {
                String[] valueItems = value.toString().split("\\s+");
                //过滤格式错误的记录
                if (valueItems.length != 4) {
                    return;
                }
                //过滤掉不需要参加join的记录
                if (joinKeySet.contains(valueItems[3])) {
                    flag.set("1");
                    joinKey.set(valueItems[3]);
                    secondPart.set(valueItems[0] + "\t" + valueItems[1] + "\t" + valueItems[2]);
                    combineValues.setFlag(flag);
                    combineValues.setJoinKey(joinKey);
                    combineValues.setSecondPart(secondPart);
                    context.write(combineValues.getJoinKey(), combineValues);
                } else {
                    return;
                }
            }
        }
    }

    public static class SemiJoinReducer extends Reducer<Text, ReduceSideJoin.CombineValues, Text, Text> {
        //存储一个分组中的左表信息
        private ArrayList<Text> leftTable = new ArrayList<Text>();
        //存储一个分组中的右表信息
        private ArrayList<Text> rightTable = new ArrayList<Text>();
        private Text secondPar = null;
        private Text output = new Text();

        /**
         * 一个分组调用一次reduce函数
         */
        @Override
        protected void reduce(Text key, Iterable<ReduceSideJoin.CombineValues> value, Context context)
                throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
            /**
             * 将分组中的元素按照文件分别进行存放
             * 这种方法要注意的问题：
             * 如果一个分组内的元素太多的话，可能会导致在reduce阶段出现OOM，
             * 在处理分布式问题之前最好先了解数据的分布情况，根据不同的分布采取最
             * 适当的处理方法，这样可以有效的防止导致OOM和数据过度倾斜问题。
             */
            for (ReduceSideJoin.CombineValues cv : value) {
                secondPar = new Text(cv.getSecondPart().toString());
                //左表tb_dim_city
                if ("0".equals(cv.getFlag().toString().trim())) {
                    leftTable.add(secondPar);
                }
                //右表tb_user_profiles
                else if ("1".equals(cv.getFlag().toString().trim())) {
                    rightTable.add(secondPar);
                }
            }
            logger.info("tb_dim_city:" + leftTable.toString());
            logger.info("tb_user_profiles:" + rightTable.toString());
            for (Text leftPart : leftTable) {
                for (Text rightPart : rightTable) {
                    output.set(leftPart + "\t" + rightPart);
                    context.write(key, output);
                }
            }
        }
    }

    private static final String INPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/join/*.txt";
    // 假设 key 已经过滤到 join_key.txt文件
    private static final String CACHE_INPUT_PAHT = "D:/GitWorkspace/hadoop_test/src/main/resources/join/join_key.txt";
    private static final String OUTPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/join/stats_semi_join";

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4");
        Configuration conf = new Configuration(); //获得配置文件对象
        Job job = Job.getInstance(conf, "SemiJoin");

        // cache file
        job.addCacheFile(new Path(CACHE_INPUT_PAHT).toUri());

        job.setJarByClass(ReduceSideJoin.class);

        FileUtils.deleteDirectory(OUTPUT_PATH, conf);
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setMapperClass(SemiJoinMapper.class);
        job.setReducerClass(SemiJoinReducer.class);

        job.setInputFormatClass(TextInputFormat.class); //设置文件输入格式
        job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格式

        //设置map的输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReduceSideJoin.CombineValues.class);

        //设置reduce的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
