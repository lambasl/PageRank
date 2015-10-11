package lab3.mr.jobs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalculateN {
	
	static class CalculateNMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private static Text outKey = new Text("N");
		private static IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(outKey, one);
		}
		
	}
	
	static class CalculateNReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			IntWritable count = new IntWritable(0);
			Iterator<IntWritable> ones = values.iterator();
			while(ones.hasNext()){
				int prev = count.get();
				count.set(prev + 1);
			}
			
			context.write(key, count);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Running RedLink Extractor");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(CalculateNMapper.class);
		job.setReducerClass(CalculateNReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, "adjacency");
		Path outputPath = new Path("count");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
	}

}
