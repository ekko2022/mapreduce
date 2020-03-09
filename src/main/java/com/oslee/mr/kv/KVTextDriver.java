package com.oslee.mr.kv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KVTextDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[] {"e:/1130/input/inputkv", "e:/output/outputkv"};
		Configuration conf = new Configuration();
		// 设置切割符
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

		// 获取job对象
		Job job = Job.getInstance(conf);
		
		// 设置jar路径
		job.setJarByClass(KVTextDriver.class);
		
		// 关联Mapper和Reducer
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		
		// 设置Mapper和reducer的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置最终输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 设置输入格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
