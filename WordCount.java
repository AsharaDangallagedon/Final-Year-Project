import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//WordCount class for counting occurences of words from a collection of poem related web pages
public class WordCount {
    //Mapper class for WordCount which calculates emits a count of one for each word
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordText = new Text();
        private boolean div = false;
        //map method for processing each line of input text
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //div tags are used in here in order to ascertain that the line in question is within a section of the input text
            if (line.matches(".*<div class=\"post_description\">.*")) {
                div = true;
            }
            if (line.trim().equals("</div>")) {
                div = false;
            }
            //the word count is only taken from the poem itself, which lies in between the <div class=\"post_description\">. and </div> tags
            if (div) {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    //regex is used to make it is easier for the reducer to count the occurence of a word with different capital letters
                    word = word.replaceAll("<[^>]*>", "");
                    word = word.replaceAll("[^a-zA-Z]+", "");
                    word = word.replaceAll("^href\\S*", "");
                    String wordLowerCase = word.toLowerCase();
                    wordText.set(wordLowerCase);
                    context.write(wordText, one);
                }
            }
        }
    }
    //reducer class is used to aggregate the count of the words
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();
        //the reduce method actuall aggregates the count of each word by using a sum variable
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count); //occurence of each word is outputted from the reducer
        }
    }
    //main method holds the configurations to run the hadoop job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
