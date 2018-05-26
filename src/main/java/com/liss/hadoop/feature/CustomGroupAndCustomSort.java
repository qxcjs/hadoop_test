package com.liss.hadoop.feature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomGroupAndCustomSort {

    private static final Logger LOG = LoggerFactory.getLogger(CustomGroupAndCustomSort.class);

    private static class MyMapper extends Mapper<LongWritable, Text, MyNewKey, LongWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilted = value.toString().split("\\s+");
            long firstNum = Long.parseLong(spilted[0]);
            long secondNum = Long.parseLong(spilted[1]);
            LOG.info("firstNum : {} , secondNum : {}", firstNum, secondNum);
            // 使用新的类型作为key参与排序
            MyNewKey newKey = new MyNewKey(firstNum, secondNum);

            context.write(newKey, new LongWritable(secondNum));
        }
    }

    private static class MyReducer extends Reducer<MyNewKey, LongWritable, LongWritable, LongWritable> {
        protected void reduce(MyNewKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(key.firstNum), new LongWritable(key.secondNum));
        }
    }

    /**
     * 自定义分组
     * 为了针对新的key类型作分组
     */
    private static class MyGroupingComparator implements RawComparator<MyNewKey> {

        /*
         * 基本分组规则：按第一列firstNum进行分组
         */
        @Override
        public int compare(MyNewKey key1, MyNewKey key2) {
            return (int) (key1.firstNum - key2.firstNum);
        }

        /*
         * @param b1 表示第一个参与比较的字节数组
         *
         * @param s1 表示第一个参与比较的字节数组的起始位置
         *
         * @param l1 表示第一个参与比较的字节数组的偏移量
         *
         * @param b2 表示第二个参与比较的字节数组
         *
         * @param s2 表示第二个参与比较的字节数组的起始位置
         *
         * @param l2 表示第二个参与比较的字节数组的偏移量
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
        }

    }

    /**
     * 自定义排序
     * 封装一个自定义类型作为key的新类型：将第一列与第二列都作为key
     */
    private static class MyNewKey implements WritableComparable<MyNewKey> {
        private long firstNum;
        private long secondNum;

        public MyNewKey() {
        }

        public MyNewKey(long first, long second) {
            firstNum = first;
            secondNum = second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(firstNum);
            out.writeLong(secondNum);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            firstNum = in.readLong();
            secondNum = in.readLong();
        }

        /*
         * 当key进行排序时会调用以下这个compreTo方法
         * 如果参数字符串等于此字符串，则返回值 0；
         * 如果此字符串小于字符串参数，则返回一个小于 0 的值；
         * 如果此字符串大于字符串参数，则返回一个大于 0 的值。
         */
        @Override
        public int compareTo(MyNewKey anotherKey) {
            long min = firstNum - anotherKey.firstNum;
            if (min != 0) {
                // 说明第一列不相等，则返回两数之间小的数
                return (int) min;
            } else {
                return (int) (secondNum - anotherKey.secondNum);
            }
        }
    }

    //分组内部进行排序，按照第二个字段进行排序
    public static class MySortComparator extends WritableComparator {
        public MySortComparator() {
            super(MyNewKey.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return 0;
        }
    }

    private static final String INPUT_PATH = "D:/GitWorkspace/spark_test/src/main/resources/GroupNumber.txt";
    private static final String OUTPUT_PATH = "D:/GitWorkspace/spark_test/src/main/resources/stats_custom_group_and_custom_sort";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(CustomGroupAndCustomSort.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(MyNewKey.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置自定义分组规则
//        job.setSortComparatorClass(MySortComparator.class);
//        job.setGroupingComparatorClass(MyGroupingComparator.class);

        // 判断output文件夹是否存在，如果存在则删除
        Path path = new Path(OUTPUT_PATH);
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
