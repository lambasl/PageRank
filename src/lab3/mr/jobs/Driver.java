package lab3.mr.jobs;

import java.io.IOException;

import lab3.mr.jobs.OutlinkAdjacencyBuilder.OutlinkAdjacencyMapper;
import lab3.mr.jobs.OutlinkAdjacencyBuilder.OutlinkAdjacencyReducer;
import lab3.mr.jobs.RedLinkExtractor.RedLinkReducer;
import mahout.inputformat.XmlInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Job1: Running RedLink Extractor....");

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(OutLinkMapperStage1.class);
		job.setReducerClass(RedLinkReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, Helper.INPUT_BUCKET + "data");
		Path outputPath = new Path(Helper.TEMP_BUCKET + "sampleout");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
		job.waitForCompletion(true);
		
		
		System.out.println("job2..Running OutlinkAdjacecnyBuilder...." );
		
		job = Job.getInstance(conf);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(OutlinkAdjacencyMapper.class);
		job.setReducerClass(OutlinkAdjacencyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, Helper.TEMP_BUCKET + "sampleout");
		outputPath = new Path(Helper.TEMP_BUCKET + "adjacency");
		FileOutputFormat.setOutputPath(job, outputPath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
		job.waitForCompletion(true);
		
		FileUtil.copyMerge(fs, outputPath, fs, new Path(Helper.OUTPUT_BUCKET + "PageRank.outlink.out"), true, conf,"");
		
		
	}

}
