package lab3.mr.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import mahout.inputformat.XmlInputFormat;

import org.apache.commons.lang.StringUtils;
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

public class RedLinkExtractor {

	static class RedLinkMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String title = StringUtils.substringBetween(value.toString(), "<title>", "</title>");
			String[] links = StringUtils.substringsBetween(value.toString(), "[[", "]]");
			//String [] links = extractLink(value.toString())
			Text titleText = new Text(replaceSpaces(title));
			if (links != null ) {
				for (String link : links) {
					if(link.contains("[[")){
						String[] link1 = link.split("\\[\\[");
						link = link1[link1.length-1];
					}
					if(link.length() >0 && !isNotWikiLink(link)&& isValidLink(link) && !link.equals(title)){
					context.write(new Text(extractLink(link)), titleText);
					}
				}
			}

			context.write(titleText, new Text("####"));

		}


	}
	
	static class RedLinkReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valIter = values.iterator();
			boolean isRedLink = true;
			StringBuilder inlinks = new StringBuilder();
			Set<String> inlinkSet = new HashSet<>();
			while (valIter.hasNext()) {
				String val = valIter.next().toString();
				if ("####".equals(val)) {
					isRedLink = false;
				} else {
					if(!inlinkSet.contains(val)){
					inlinks.append(val).append("\t");
					inlinkSet.add(val);
					}
				}
			}
			if (!isRedLink) {
				if (inlinks.length() > 0 && (inlinks.charAt(inlinks.length() - 1) == '\t')) {
					inlinks.deleteCharAt(inlinks.length() - 1);
				}
				context.write(key, new Text(inlinks.toString()));

			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Running RedLink Extractor");

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
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
		job.waitForCompletion(true);
		
	}

	private static String replaceSpaces(String str) {
		if (str == null) {
			return null;
		} else {
			return sweetify(str.replaceAll(" ", "_"));
		}
	}

	private static String extractLink(String linkText) {
		String title = null;
		if (linkText.contains("|")) {
			String[] splits = linkText.split("\\|");
			title = splits[0];
		} else {
			title = linkText;
		}
		return replaceSpaces(title);
		
	}
	
	private static boolean isValidLink(String link) {
		boolean isValidLink = true;
		link = link.trim();
		if(link.length() ==0){
			isValidLink = false;
		}
		
		//interwiki links
		if(StringUtils.countMatches(link, ":") >= 2){
			isValidLink = false;
		}
		
		//section linking
		if(link.contains("#")){
			isValidLink = false;
		}
		
		//section linking
		if(link.contains("comment>") || link.contains("<text>")){
			isValidLink = false;
		}
		
		//subpage links
		if(link.startsWith("/") || link.startsWith("../")){
			isValidLink = false;
		}
		
		//links to fliles
		if(link.startsWith("File:") || link.startsWith("Image:") || link.startsWith("image:")){
			isValidLink = false;
		}
		
		//links ends with space
		if(link.endsWith(" ")){
			isValidLink = false;
		}
		
		
		return isValidLink;
	}
	
    private static boolean isNotWikiLink(String aLink) {
        char firstChar = aLink.charAt(0);
        
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;
        
        if( aLink.contains(":")) return true; // Matches: external links and translations links
        if( aLink.contains(",")) return true; // Matches: external links and translations links
        if( aLink.contains("&")) return true;
        
        return false;
    }
    
    private static String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
    }
    
    private String getWikiPageFromLink(String aLink){
        if(isNotWikiLink(aLink)) return null;
        
        int start = aLink.startsWith("[[") ? 2 : 1;
        int endLink = aLink.indexOf("]");

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        
        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }
        
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = sweetify(aLink);
        
        return aLink;
    }

}
