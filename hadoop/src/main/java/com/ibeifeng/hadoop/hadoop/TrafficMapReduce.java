package com.ibeifeng.hadoop.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TrafficMapReduce {
	public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            
            Job job = Job.getInstance(conf);
            job.setJarByClass(TrafficMapReduce.class);
            
            job.setMapperClass(TrafficMapper.class);
            job.setMapOutputKeyClass(Traffic.class);
            job.setMapOutputValueClass(Text.class);
                     
    		FileInputFormat.setInputPaths(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		   		
    		job.waitForCompletion(true);
	}

	public static class TrafficMapper extends Mapper<LongWritable, Text, Traffic, Text> {
		Traffic traffic =new Traffic(); 
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			int upPackNum = Integer.valueOf(split[6]);
			int downPackNum = Integer.valueOf(split[7]);
			int upPayLoad = Integer.valueOf(split[8]);
			int downPayLoad = Integer.valueOf(split[9]);
			String phoneNum = split[1];
			traffic.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
            v.set(phoneNum);
            context.write(traffic, v);
		}

	}
}
