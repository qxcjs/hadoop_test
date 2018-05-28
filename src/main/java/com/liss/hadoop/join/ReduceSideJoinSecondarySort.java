package com.liss.hadoop.join;

import com.liss.hadoop.utils.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 二次排序实现 Reduce Side Join
 */
public class ReduceSideJoinSecondarySort {
    private static final Logger LOG = LoggerFactory.getLogger(ReduceSideJoinSecondarySort.class);

    public static class TextPair implements WritableComparable<TextPair> {
        private Text first;//Text 类型的实例变量 first
        private Text second;//Text 类型的实例变量 second

        public TextPair() {
            set(new Text(), new Text());
        }

        public TextPair(String first, String second) {
            set(new Text(first), new Text(second));
        }

        public TextPair(Text first, Text second) {
            set(first, second);
        }

        public void set(Text first, Text second) {
            this.first = first;
            this.second = second;
        }

        public Text getFirst() {
            return first;
        }

        public Text getSecond() {
            return second;
        }

        //将对象转换为字节流并写入到输出流out中
        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
        }

        //从输入流in中读取字节流反序列化为对象
        public void readFields(DataInput in) throws IOException {
            first.readFields(in);
            second.readFields(in);
        }

        @Override
        public int hashCode() {
            return first.hashCode() * 163 + second.hashCode();
        }


        @Override
        public boolean equals(Object o) {
            if (o instanceof TextPair) {
                TextPair tp = (TextPair) o;
                return first.equals(tp.first) && second.equals(tp.second);
            }
            return false;
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }


        @Override
        public int compareTo(TextPair o) {
            if (!first.equals(o.first)) {
                return first.compareTo(o.first);
            } else if (!second.equals(o.second)) {
                return second.compareTo(o.second);
            } else {
                return 0;
            }
        }
    }


    //    JoinStationMapper 处理来自气象站数据
    public static class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split("\\s+");//解析天气记录数据
            if (arr.length == 2) {//满足这种数据格式
                //key=气象站id  value=气象站名称
                context.write(new TextPair(arr[0], "0"), new Text(arr[1]));
            }
        }
    }

    public static class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split("\\s+");//解析天气记录数据
            if (arr.length == 3) {
                //key=气象站id  value=天气记录数据
                context.write(new TextPair(arr[0], "1"), new Text(arr[1] + "\t" + arr[2]));
            }
        }
    }

    public static class KeyPartitioner extends Partitioner<TextPair, Text> {
        public int getPartition(TextPair key, Text value, int numPartitions) {
//            &是位与运算
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(TextPair.class, true);
        }

        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPair ip1 = (TextPair) w1;
            TextPair ip2 = (TextPair) w2;
            Text l = ip1.getFirst();
            Text r = ip2.getFirst();
            return l.compareTo(r);
        }
    }

    public static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> list = new LinkedList<>();

            Iterator<Text> iter = values.iterator();
            Text stationName = new Text(iter.next());//气象站名称

            list.add(new String(stationName.getBytes())); // 日志

            while (iter.hasNext()) {
                Text record = iter.next();//天气记录的每条数据
                Text outValue = new Text(stationName.toString() + "\t" + record.toString());
                context.write(key.getFirst(), outValue);

                list.add(new String(record.getBytes())); // 日志
            }
            LOG.info("key first : {} , key second : {} , values : {}", key.first, key.second, list.toString());
        }
    }

    private static final String INPUT_PATH_STATION = "D:/GitWorkspace/hadoop_test/src/main/resources/weather/station.txt";
    private static final String INPUT_PATH_RECORD = "D:/GitWorkspace/hadoop_test/src/main/resources/weather/record.txt";
    private static final String OUTPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/weather/stats_reduce_side_join_secondary_sort";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();// 读取配置文件

        Job job = Job.getInstance(conf, "ReduceSideJoinSecondarySort");// 新建一个任务

        FileUtils.deleteDirectory(OUTPUT_PATH, conf);

        job.setJarByClass(ReduceSideJoinSecondarySort.class);// 主类

        Path recordInputPath = new Path(INPUT_PATH_RECORD);//天气记录数据源
        Path stationInputPath = new Path(INPUT_PATH_STATION);//气象站数据源
        Path outputPath = new Path(OUTPUT_PATH);
        // 两个输入类型
        MultipleInputs.addInputPath(job, recordInputPath, TextInputFormat.class, JoinRecordMapper.class);//读取天气记录Mapper
        MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);//读取气象站Mapper
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setReducerClass(JoinReducer.class);// Reducer

        job.setPartitionerClass(KeyPartitioner.class);//自定义分区
        job.setGroupingComparatorClass(GroupingComparator.class);//自定义分组

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
