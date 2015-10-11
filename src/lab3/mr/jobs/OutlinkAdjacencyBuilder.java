package lab3.mr.jobs;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OutlinkAdjacencyBuilder {

	static class OutlinkAdjacencyMapper extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (value == null || value.toString().length() == 0) {
				return;
			}
			String[] values = value.toString().split("\\s+");
			Text toNodeText = new Text(values[0]);
			context.write(toNodeText, new Text("####"));
			if (values.length > 1) {
				HashSet<String> valueSet = new HashSet<String>(Arrays.asList(Arrays.copyOfRange(values, 1,
						values.length)));
				Iterator<String> valIter = valueSet.iterator();
				while (valIter.hasNext()) {
					Text t = new Text(valIter.next());
					
					context.write(t,toNodeText);
					context.write( t,  new Text("####"));
				}
			}
		}
	}

	static class OutlinkAdjacencyReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIter = values.iterator();
			StringBuilder valStr = new StringBuilder("");
			while (valuesIter.hasNext()) {
				String nextVal = valuesIter.next().toString();
				if ("####".equals(nextVal))
					continue;
				valStr.append(nextVal).append("\t");
			}
			if (valStr.length() > 0 && valStr.charAt(valStr.length() - 1) == '\t') {
				valStr.deleteCharAt(valStr.length() - 1);
			}
			context.write(key,new Text(valStr.toString()));
		}
	}

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Running RedLink Extractor");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(OutlinkAdjacencyMapper.class);
		job.setReducerClass(OutlinkAdjacencyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, "sampleout");
		Path outputPath = new Path("adjacency");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		job.submit();
	}
}
