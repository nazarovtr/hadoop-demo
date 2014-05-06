package ru.projectsoft.hadoopdemo.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class DemoReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        long result = 0;
        while (values.hasNext()) {
            result += values.next().get();
        }
        output.collect(new Text(key), new LongWritable(result));
    }
}
