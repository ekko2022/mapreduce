package com.oslee.mr.cache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	HashMap<Object, Object> pdMap = new HashMap<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 缓存小表
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())) {
			// 1 切割
			String[] fields = line.split("\t");
			
			pdMap.put(fields[0], fields[1]);
		}
		
		// 2 关闭资源
		IOUtils.closeStream(reader);
	}
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		// 1 获取一行
		String line = value.toString();
		
		// 2 截取
		String[] fields = line.split("\t");
		
		// 3 获取产品id
		String pId = fields[1];
		
		// 4 获取商品名称
		Object pdName = pdMap.get(pId);
//		String pdName = pdMap.get(pId);
		
		// 5 拼接
		k.set(line + "\t"+ pdName);
		
		// 6 写出
		context.write(k, NullWritable.get());

	}
}
