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


public class RegexSearch {
    public static class SearchMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text locationText = new Text();
        private Pattern pattern;
        private boolean divTag;
        private Pattern startPattern = Pattern.compile("<div id=\"main-text\">");
        private Pattern endPattern = Pattern.compile("<div id=\"bibliography\">");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            Matcher startMatcher = startPattern.matcher(value.toString());
            Matcher endMatcher = endPattern.matcher(value.toString());
            if (startMatcher.matches()) {
                divTag = true;
            }
            if (endMatcher.matches()) {
                divTag = false;
            }
            if (divTag) {
                Matcher contentMatcher = Pattern.compile("\\b(?!https?|href)\\w*(?i)h\\w*").matcher(value.toString());
                while (contentMatcher.find()) {
                    String matchedString = contentMatcher.group();
                    locationText.set("Line " + key);
                    context.write(new Text(matchedString), locationText); 
                }
                
            }  
        }
    }

    public static class SearchReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
            StringBuilder locations = new StringBuilder();
            ArrayList<String> uniqueValues = new ArrayList<>();
            for (Text value : values) {
                String valueString = value.toString();
                if (!uniqueValues.contains(valueString)) {
                    uniqueValues.add(valueString);
                    locations.append(valueString).append(", ");
                }
            }
            if (locations.length() > 0) {
                locations.deleteCharAt(locations.length() - 2);
            }
            context.write(key, new Text(locations.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Regex Search");
        job.setJarByClass(RegexSearch.class);
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