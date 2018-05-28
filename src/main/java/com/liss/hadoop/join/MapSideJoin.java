package com.liss.hadoop.join;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

import com.liss.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * left outer join
 */
public class MapSideJoin {
    private static final Logger logger = LoggerFactory.getLogger(MapSideJoin.class);

    public static class LeftOutJoinMapper extends Mapper<Object, Text, Text, Text> {

        private HashMap<String, String> city_info = new HashMap<String, String>();
        private Text outPutKey = new Text();
        private Text outPutValue = new Text();
        private String mapInputStr = null;
        private String mapInputSpit[] = null;
        private String city_secondPart = null;

        /**
         * 此方法在每个map task开始之前执行，这里主要用作从DistributedCache
         * 中取到left_table文件，并将里边记录取出放到内存中。
         */
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            BufferedReader br = null;
            //获得当前作业的DistributedCache相关文件
            URI[] uris = context.getCacheFiles();
            String cityInfo = null;
            for (URI uri : uris) {
                Path path = new Path(uri.getPath());
                if (path.toString().endsWith("left_table.txt")) {
                    //读缓存文件，并放到memory中
                    br = new BufferedReader(new FileReader(path.toString()));
                    while (null != (cityInfo = br.readLine())) {
                        String[] cityPart = cityInfo.split("\\s+", 5);
                        if (cityPart.length == 5) {
                            city_info.put(cityPart[0], cityPart[1] + "\t" + cityPart[2] + "\t" + cityPart[3] + "\t" + cityPart[4]);
                        }
                    }
                }
            }
        }

        /**
         * Map端的实现相当简单，直接判断tb_user_profiles.dat中的
         * cityID是否存在我的map中就ok了，这样就可以实现Map Join了
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //排掉空行
            if (value == null || value.toString().equals("")) {
                return;
            }
            mapInputStr = value.toString();
            mapInputSpit = mapInputStr.split("\\s+", 4);
            //过滤非法记录
            if (mapInputSpit.length != 4) {
                return;
            }
            //判断链接字段是否在map中存在
            city_secondPart = city_info.get(mapInputSpit[3]);
            if (city_secondPart != null) {
                this.outPutKey.set(mapInputSpit[3]);
                this.outPutValue.set(city_secondPart + "\t" + mapInputSpit[0] + "\t" + mapInputSpit[1] + "\t" + mapInputSpit[2]);
                context.write(outPutKey, outPutValue);
            }
        }
    }


    private static final String INPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/join/right_table.txt";
    // 假设left table 很小
    private static final String CACHE_INPUT_PAHT = "D:/GitWorkspace/hadoop_test/src/main/resources/join/left_table.txt";
    private static final String OUTPUT_PATH = "D:/GitWorkspace/hadoop_test/src/main/resources/join/stats_map_side_join";

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4");
        Configuration conf = new Configuration(); //获得配置文件对象
        Job job = Job.getInstance(conf, "MapSideJoin");

        // cache file
        job.addCacheFile(new Path(CACHE_INPUT_PAHT).toUri());
        job.setJarByClass(ReduceSideJoin.class);

        FileUtils.deleteDirectory(OUTPUT_PATH, conf);
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setMapperClass(LeftOutJoinMapper.class);

        job.setInputFormatClass(TextInputFormat.class); //设置文件输入格式
        job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格式

        //设置map的输出key和value类型
        job.setMapOutputKeyClass(Text.class);

        //设置reduce的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
