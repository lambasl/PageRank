package lab3.mr.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankPrep {

	private final static int N = getN();
	
	static class PageRankPrepMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] links = value.toString().split("\\s+");
			StringBuilder linkswithPR = new StringBuilder();
			linkswithPR.append(Double.valueOf(1.0/N).toString()).append("\t");
			for(int i=1; i<links.length; i++){
				linkswithPR.append(links[i]).append("\t");
			}
			linkswithPR.deleteCharAt(linkswithPR.length()-1);
			context.write(new Text(links[0]), new Text(linkswithPR.toString()));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Running PageRankPrep");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(PageRankPrepMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, "adjacency");
		Path outputPath = new Path("pagerankout0");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
	
	}
	
	private static int getN(){
		//read N from prev job's output file and return
		return 20000;
	}
}
