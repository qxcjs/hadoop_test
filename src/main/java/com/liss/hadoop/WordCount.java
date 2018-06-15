package com.liss.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
//            LOG.info("map value:"+value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4");
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.maps","2");
        conf.set("mapreduce.job.reduces","2");
        conf.set("mapreduce.input.fileinputformat.split.minsize","2");
        conf.set("mapreduce.task.io.sort.mb","25");
        conf.set("mapreduce.map.combine.minspills","3");
        conf.set("mapreduce.reduce.memory.totalbytes","99614720");
        conf.set("mapreduce.task.io.sort.factor","2");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent","0.4");
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent","0.5");
        Job job = Job.getInstance(conf, "word count");
//        job.setCacheFiles(null);
//        job.setGroupingComparatorClass(null);
//        job.setSortComparatorClass(null);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 判断output文件夹是否存在，如果存在则删除
        Path path = new Path("C:\\Users\\Administrator\\Desktop\\sparkpartiton\\stats");
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\sparkpartiton\\1987.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\sparkpartiton\\stats"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
