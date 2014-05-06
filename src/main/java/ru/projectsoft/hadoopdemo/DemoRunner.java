package ru.projectsoft.hadoopdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import ru.projectsoft.hadoopdemo.mr.DemoMapper;
import ru.projectsoft.hadoopdemo.mr.DemoReducer;

import java.io.IOException;

public class DemoRunner {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "cloudera");
        Configuration configuration = new Configuration();
//        configuration.set("mapred.jobtracker", "192.168.5.102:8021");
//        configuration.set("fs.defaultFS", "192.168.5.102:8020");
        JobConf jobConf = new JobConf(configuration);
        jobConf.setJarByClass(DemoRunner.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(LongWritable.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);

        jobConf.setMapperClass(DemoMapper.class);
        jobConf.setReducerClass(DemoReducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path("/user/cloudera/demo_input/"));
        FileOutputFormat.setOutputPath(jobConf, new Path("/user/cloudera/demo_output/"));

        JobClient.runJob(jobConf);
    }
}
