package lab3.mr.jobs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrintPageRank {
	
	private static int N = 20000;
	
	static class PrintPageRankMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DoubleWritable, Text>.Context context) throws IOException,
				InterruptedException {
			
			String[] values = value.toString().split("\\s+");
			double pageRank = Double.valueOf(values[1]);
			Text page = new Text(values[0]);
			if(pageRank > 5.0/N)
				context.write(new DoubleWritable(pageRank), page);
		}
	}
	
	static class PrintPageRankReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,
				Reducer<DoubleWritable, Text, Text, DoubleWritable>.Context context) throws IOException,
				InterruptedException {
			
			Iterator<Text> valIter = values.iterator();
			while(valIter.hasNext()){
				context.write(valIter.next(), key);
			}
		}
	}
	
	static class ReverseDoubleComparator implements RawComparator<DoubleWritable>{

		@Override
		public int compare(DoubleWritable o1, DoubleWritable o2) {
			return -1 * o1.compareTo(o2);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -1 * DoubleWritable.Comparator.compareBytes(b1, s1, l1, b2, s2, l2);
		}

		
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(PrintPageRankMapper.class);
		job.setReducerClass(PrintPageRankReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setSortComparatorClass(ReverseDoubleComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job, "pagerankout8");
		Path outputPath = new Path("pagerankout");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
		job.waitForCompletion(true);
	}

}
