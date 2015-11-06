package com.aamend.hadoop.MapReduce;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase
		implements Mapper<LongWritable,Text,Text,IntWritable>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {
		String s = value.toString();
		for(String word:s.split(" ")){
			if(word.length()>0){
				output.collect(new Text(word), new IntWritable(1));
			}
		}
		
	}
	
}


/*
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
public class WordCountMapper extends
        Mapper<Object, Text, Text, IntWritable> {
 
    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
 
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
 
        String[] csv = value.toString().split(",");
        for (String str : csv) {
            word.set(str);
            context.write(word, ONE);
        }
    }
}*/
