package com.liss.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerWordCount {
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws java.io.IOException, InterruptedException {
            String line = value.toString();
            String[] spilted = line.split(" ");
            for (String word : spilted) {
                context.write(new Text(word), new LongWritable(1L));
                // 为了显示效果而输出Mapper的输出键值对信息
                System.out.println("Mapper输出<" + word + "," + 1 + ">");
            }
        }
    }

    public static class MyReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(Text key,
                              java.lang.Iterable<LongWritable> values,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws java.io.IOException, InterruptedException {
            // 显示次数表示redcue函数被调用了多少次，表示k2有多少个分组
            System.out.println("Reducer输入分组<" + key.toString() + ",N(N>=1)>");

            long count = 0L;
            for (LongWritable value : values) {
                count += value.get();
                // 显示次数表示输入的k2,v2的键值对数量
                System.out.println("Reducer输入键值对<" + key.toString() + ","
                        + value.get() + ">");
            }
            context.write(key, new LongWritable(count));
            System.out.println("Reducer输出键值对<" + key.toString() + "," + count
                    + ">");

        }
    }

    public static class MyCombiner extends
            Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(
                Text key,
                java.lang.Iterable<LongWritable> values,
                org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws java.io.IOException, InterruptedException {
            // 显示次数表示规约函数被调用了多少次，表示k2有多少个分组
            System.out.println("Combiner输入分组<" + key.toString() + ",N(N>=1)>");
            long count = 0L;
            for (LongWritable value : values) {
                count += value.get();
                // 显示次数表示输入的k2,v2的键值对数量
                System.out.println("Combiner输入键值对<" + key.toString() + ","
                        + value.get() + ">");
            }
            context.write(key, new LongWritable(count));
            // 显示次数表示输出的k2,v2的键值对数量
            System.out.println("Combiner输出键值对<" + key.toString() + "," + count
                    + ">");
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "combine word count");
        job.setNumReduceTasks(2);
        job.setJarByClass(CombinerWordCount.class);
        job.setMapperClass(MyMapper.class);
        /**
         *  第一种 不设置Combiner类，直接将下面这行代码注释掉
         *  第二种 直接以Reduce类作为Combiner类
         *  第三种 自定义一个MyCombiner类做计算
         */
//        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("D:/GitWorkspace/spark_test/src/main/resources/WordCount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:/GitWorkspace/spark_test/src/main/resources/stats_combine"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
