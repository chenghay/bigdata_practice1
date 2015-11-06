package com.aamend.hadoop.MapReduce;
 
import java.io.IOException;


//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;//added by me
import org.apache.hadoop.util.Tool;//added by me
import org.apache.hadoop.util.ToolRunner;// added by me
import org.apache.hadoop.fs.Path;// added by me
import org.apache.hadoop.io.IntWritable;// added by me
import org.apache.hadoop.io.Text;//added by me
import org.apache.hadoop.mapred.JobConf;//added by me
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; conflicts with the one below
import org.apache.hadoop.mapred.FileInputFormat;// added by me
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; conflicts with the one below
import org.apache.hadoop.mapred.FileOutputFormat;//added by me
import org.apache.hadoop.mapred.JobClient;// added by me

//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class WordCount extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length<2){
			System.out.println("please give input and output directory!");
			return -1;
		}
		JobConf conf = new JobConf(WordCount.class);
	
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
		
		conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        JobClient.runJob(conf);
		return 0;
	}
 
	
/*	
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(WordCountMapper.class);
 
        // Setup MapReduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);
 
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
*/
	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}
 
}
