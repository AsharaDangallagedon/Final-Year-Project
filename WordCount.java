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
/**
 * @author Ashara Dangallage don
 * WordCount class counts the occurences of worsd from a collection of poetry related webpages
 */
public class WordCount {
    /**
     * Mapper class is used to count words in the input text file
     * The output is the word itself alongside its count (1) 
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordText = new Text();
        private boolean div = false;
        /**
         * Map method processes each line of the input text
         * @param key The key represents the byte offset of the input data line
         * @param values The values are the contents of the line
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when reading from the file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
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
    /**
     * Reducer class is used for aggregating the word occurences 
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();
        /**
         * Reduce method is used to aggregate the count of each word by using a sum variable
         * @param key The key represents a word
         * @param values The values are a collection of word occurences
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when writing to the output file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            //occurence of each word is outputted from the reducer
            context.write(key, count); 
        }
    }
    /**
     * Main method holds the configurations to run the hadoop job
     * @param args args specifies the input and output paths
    */
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
