package com.playground.moviedb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MoviePopularity { 
	
	public static class MovieMapper extends Mapper<Object,Text,Text,IntWritable> {
		
		private Text ratingType = new Text();
		private static final IntWritable one = new IntWritable(1);
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String ratingTypeStr = value.toString().split("\t")[1];
			ratingType.set(ratingTypeStr);
			context.write(new Text(ratingType),one);
		}
		
	}
	
	public static class MovieReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
		
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable i : values) {
				sum += i.get();
			}
			result.set(sum);
			context.write(result, key);
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(MoviePopularity.class);
	    job.setMapperClass(MovieMapper.class);
	    job.setCombinerClass(MovieReducer.class);
	    job.setReducerClass(MovieReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}

}
