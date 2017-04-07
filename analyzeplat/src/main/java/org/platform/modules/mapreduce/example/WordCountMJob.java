package org.platform.modules.mapreduce.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountMJob {
	
	private static void configureJob(Job job) {
		job.setJarByClass(WordCountMJob.class);
		
		job.setMapperClass(WordCountMMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(HashPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false); 
		conf.setBoolean("mapreduce.reduce.speculative", false); 
		conf.set("mapreduce.framework.name", "yarn"); 
		conf.set("hadoop.job.user", "dataplat"); 
		try {
			String[] inputArgs = new GenericOptionsParser(
					conf, args).getRemainingArgs();
			if (inputArgs.length != 2) {
				System.out.println("error, please input two path. input and output");
				System.exit(2);
			}
			Job job = Job.getInstance(conf, "WordSortMR");
			
			FileInputFormat.addInputPath(job, new Path(inputArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(inputArgs[1]));
			
			configureJob(job);
			
			System.out.println(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class WordCountMMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	protected MultipleOutputs<NullWritable, Text> multipleOutputs = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value.toString().equalsIgnoreCase("hadoop")) {
			multipleOutputs.write(NullWritable.get(), new Text(value), "h1");
		} else {
			multipleOutputs.write(NullWritable.get(), new Text(value), "h2");
		}
	}
}

