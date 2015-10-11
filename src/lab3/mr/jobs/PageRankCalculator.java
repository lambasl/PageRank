package lab3.mr.jobs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankCalculator {
	private static String FLOAT_REGEX = "^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$";
	private static final double d = 0.85;
	private static final int N = 20000;

	static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String values[] = value.toString().split("\\s+");
			Text keyText = new Text(values[0]);
			double totalPR = Double.valueOf(values[1]);
			double individualPR ;
			if(values.length == 2){
				individualPR = totalPR;
			}else{
				individualPR = totalPR / (values.length - 2);
			}
			
			Text individualPRText = new Text(String.valueOf(individualPR));
			StringBuilder adjacencyList = new StringBuilder();
			for (int i = 2; i <= values.length - 1; i++) {
				context.write(new Text(values[i]), individualPRText);
				adjacencyList.append(values[i]).append("\t");
			}
			if(values.length == 2){
				//there are no contributing elements, page rant of page remains same
				
			}
			if (adjacencyList.length() > 0 && adjacencyList.charAt(adjacencyList.length() -1) == '\t') {
				adjacencyList.deleteCharAt(adjacencyList.length() - 1);
			}
			context.write(keyText, new Text(adjacencyList.toString()));
			
		}
	}

	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Iterator<Text> valIter = values.iterator();

			double pageRankSum = 0.0;
			String adjacentcyList = "";
			while (valIter.hasNext()) {
				String value = valIter.next().toString();
				if (value.matches(FLOAT_REGEX)) {
					pageRankSum += Double.valueOf(value);
				} else {
					adjacentcyList = value;
				}
			}
			context.write(key, new Text(String.valueOf((1-d)/N + d*pageRankSum + "\t" + adjacentcyList)));

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("RageRankCalulator");

		for (int i = 1; i <= 8; i++) {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, "pagerankout" + (i - 1));
			Path outputPath = new Path("pagerankout" + i);
			FileOutputFormat.setOutputPath(job, outputPath);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			job.submit();
			job.waitForCompletion(true);
		}
	}
}
