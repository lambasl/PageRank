package lab3.mr.jobs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OutLinkMapperStage1 extends  Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    		throws IOException, InterruptedException {
    	
        String page = value.toString();
        String txt = "";
        String title = "";

        title = sweetify(page.substring(page.indexOf("<title>")+7, page.indexOf("</title>")).replace(" ", "_"));

        int startIndex = page.indexOf("<text");
        int endIndex = page.indexOf("</text>");
        if ( startIndex != -1 && endIndex != -1)
            txt = page.substring(startIndex, endIndex );

        //Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Pattern p = Pattern.compile("\\[\\[([^\\[]*?)\\]\\]");
        Matcher m = p.matcher(txt);

        //This part handles the red links
        context.write(new Text(title), new Text("####"));

        while(m.find()) {

            String temp = m.group(1);

//            if(temp != null & !temp.isEmpty() && temp.charAt(0) != '#' && temp.charAt(0) != ',' && temp.charAt(0) != '.' && temp.charAt(0) != '&'
//                    && temp.charAt(0) != '\\' && temp.charAt(0) != '-' && temp.charAt(0) != '{'
//                    && !temp.contains("&") && !temp.contains(":") && !temp.contains(","))
//            {
            if(temp != null && !temp.isEmpty())
            {
                if(temp.contains("|")){
                    String outlink = sweetify(temp.substring(0, temp.indexOf("|")).replace(" ", "_"));
                    if ( !outlink.equals(title))
                        context.write(new Text(outlink), new Text(title) );
                }
                else{
                    String outlink = sweetify(temp.replace(" ","_"));
                    if ( !outlink.equals(title))
                        context.write( new Text(outlink), new Text(title) );
                }
            }
        }
        
        
 
    }
    
    private static String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&").replace("quot", "");

        return aLinkText;
    }
    
}