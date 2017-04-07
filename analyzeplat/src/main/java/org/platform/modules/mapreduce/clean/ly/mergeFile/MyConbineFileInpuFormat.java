package org.platform.modules.mapreduce.clean.ly.mergeFile;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class MyConbineFileInpuFormat extends  CombineFileInputFormat<LongWritable,Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		 CombineFileSplit combineFileSplit = (CombineFileSplit) split;
		 CombineFileRecordReader<LongWritable,Text> recordReader = 
				 new CombineFileRecordReader<LongWritable,Text>(combineFileSplit, context, MyCombineFileRecordReader.class);
		 try {
			recordReader.initialize(combineFileSplit, context);
		} catch (InterruptedException e) {
			new RuntimeException("Error to initialize CombineSmallfileRecordReader.");
		}
		 return  recordReader;
	}
	
}
