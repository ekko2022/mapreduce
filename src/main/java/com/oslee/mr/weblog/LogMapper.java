package com.oslee.mr.weblog;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 1 获取一行
		String line = value.toString();
		
		// 2 解析数据
		boolean result = parseLog(line, context);
		
		if(!result) {
			return;
		}
		
		// 3 解析通过
		context.write(value, NullWritable.get());
	}

	private boolean parseLog(String line, Context context) {
		
		// 1 截取
		String[] fields = line.split(" ");
		
		// 2 日志长度大于11的为合法
		if (fields.length > 11) {

			// 系统计数器
			context.getCounter("map", "true").increment(1);
			return true;
		} else {
			context.getCounter("map", "false").increment(1);
			return false;
		}

	}
}
