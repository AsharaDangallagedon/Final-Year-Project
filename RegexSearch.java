import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RegexSearch {
    public static class SearchMapper extends Mapper<Object, Text, Text, Text> {
        private Text matchText = new Text();
        private Text locationText = new Text();
        private Pattern pattern;
        private boolean div;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = Pattern.compile("\\bh\\w*").matcher(value.toString());
            int lineNumber = 0;
            while (matcher.find()) {
                String matchedString = matcher.group();
                matchText.set(matchedString);
                locationText.set("Line " + lineNumber + " Position " + matcher.start());
                context.write(locationText, matchText); 
                lineNumber++;
            }          
        }
    }

    public static class SearchReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Regex Search");
        job.setJarByClass(RegexSearch.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
