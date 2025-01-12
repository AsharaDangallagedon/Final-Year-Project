import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * @author Ashara Dangallage don
 * RegexSearch class searches for strings matching a particular regex expression from a collection of philosophy related webpages
 */
public class RegexSearch {
    /**
     * Mapper class is used to search for strings matching the regex expression from the input text
     */
    public static class SearchMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text locationText = new Text();
        private boolean divTag=false;
        //these 2 div tags are later used in order to ascertain that only the main text is being processed
        private Pattern startPattern = Pattern.compile("<div id=\"main-text\">"); 
        private Pattern endPattern = Pattern.compile("<div id=\"bibliography\">");
        /**
         * Map method processes each line of the input text
         * @param key The key represents the line number of the input data line
         * @param values The values are the contents of the line
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when reading from the file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            Matcher startMatcher = startPattern.matcher(value.toString());
            Matcher endMatcher = endPattern.matcher(value.toString());
            Configuration conf = context.getConfiguration();
            String regexPattern = conf.get("regex pattern");
            //checks if the current line is in between two tags, this is done to determine if only the main text is being processed
            if (startMatcher.matches()) {
                divTag = true;
            }
            if (endMatcher.matches()) {
                divTag = false;
            }
            if (divTag) {
                Matcher contentMatcher = Pattern.compile(regexPattern).matcher(value.toString());
                //matched strings are extracted and then emitted
                while (contentMatcher.find()) {
                    String matchedString = contentMatcher.group();
                    //key represents the line number and not the byte offset
                    locationText.set("Line " + key); 
                    context.write(new Text(matchedString), locationText); 
                }           
            }  
        }
    }
    /**
     * Reducer class is used for aggregating the matching strings and their locations
     */
    public static class SearchReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * Reduce method is used to aggregate matching strings and their locations
         * @param key The key represents a matched string
         * @param values The values are a collection of the locations of the matched string in the input text
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when writing to the output file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
            StringBuilder locations = new StringBuilder();
            //only unique values are added to the arraylist in order to avoid any duplicate entries in the output 
            ArrayList<String> uniqueValues = new ArrayList<>();
            for (Text value : values) {
                String valueString = value.toString();
                if (!uniqueValues.contains(valueString)) {
                    uniqueValues.add(valueString);
                    locations.append(valueString).append(", ");
                }
            }
            //the trailing comma and space is removed from locations
            if (locations.length() > 0) {
                locations.deleteCharAt(locations.length() - 2);
            }
            //the strings alongside their locations are outputted
            context.write(key, new Text(locations.toString()));
        }
    }
    /**
     * Main method holds the configurations required to run the hadoop job
     * @param args args specifies the input and output paths
    */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("regex pattern", args[2]); 
        Job job = Job.getInstance(conf, "Regex Search");
        job.setJarByClass(RegexSearch.class);
        //LineNumberInputFormat.java has been modified to calculate the line number rather than the byte offset
        job.setInputFormatClass(LineNumberInputFormat.class); 
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}