package com.liss.hadoop.feature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

public class DefaultGroupAndSort {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultGroupAndSort.class);

    private static class MyMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilted = value.toString().split("\\s+");
            long firstNum = Long.parseLong(spilted[0]);
            long secondNum = Long.parseLong(spilted[1]);
            LOG.info("firstNum : {} , secondNum : {}",firstNum,secondNum);
            context.write(new LongWritable(firstNum), new LongWritable(secondNum));
        }
    }

    private static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                context.write(key, value);
            }
        }
    }

    private static final String INPUT_PATH = "D:/GitWorkspace/spark_test/src/main/resources/GroupNumber.txt";
    private static final String OUTPUT_PATH = "D:/GitWorkspace/spark_test/src/main/resources/stats_default_group_and_sort";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(DefaultGroupAndSort.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
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
