package ru.projectsoft.hadoopdemo.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class DemoMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable ignoredKey, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] splitKey = value.toString().split("\t");
        if (splitKey.length > 2) {
            output.collect(new Text(splitKey[1]), new LongWritable(1L));
        }
    }
}
