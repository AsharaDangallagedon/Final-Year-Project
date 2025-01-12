import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * @author Ashara Dangallage don
 * TradingRange class calculates the frequency distribution of mass excess uncertainty values
 */
public class NuclearDecayDistribution {
    /**
     * Mapper class is used to emit each mass excess uncertainty value alongside its occurence
     */
    public static class DistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text massExcessUncertainty = new Text();
        private IntWritable occurence = new IntWritable(1);
        /**
         * Map method iterates through each entry in the massExcesUncertainty column
         * @param key The key represents the byte offset of the input data line
         * @param values The values are the contents of the line
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when reading from the file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void map(LongWritable key, Text currentline, Context context) throws IOException, InterruptedException {
            String[] columns = currentline.toString().split(",");
            //ensuring that there are enough columns in the dataset to emit the key-value pairs
            if (columns.length >= 11) {
                try {
                    massExcessUncertainty.set(columns[10]);
                    context.write(massExcessUncertainty, occurence);
                } catch (NumberFormatException e) {
                    System.out.println("There was a problem with the value: " + "\\\"" + massExcessUncertainty + "\\\"");
                }
            }
        }
    }
    /**
     * Reducer class aggregates the occurence counts of mass excess uncertainty values
     */
    public static class DistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable cumulativefreq = new IntWritable();
        /**
         * Reduce method is used to calculate the cumulative frequency of each mass excess value
         * @param key The key represents a mass excess uncertainty value
         * @param values The values are a collection of occurence values
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when writing to the output file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //two values (AP and massExcessUncertainty) are the only non-numerical values that show up in the massExcessUncertainty column
            if (!key.toString().equals("AP") && !key.toString().equals("massExcessUncertainty")) {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                cumulativefreq.set(sum);
                context.write(key, cumulativefreq);
            }
        }
    }
        /**
         * Main method holds the configurations required to execute a hadoop job
         * @param args args specifies the input and output paths
        */
        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distribution calculation");
        job.setJarByClass(NuclearDecayDistribution.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(DistributionMapper.class);
        job.setCombinerClass(DistributionReducer.class);
        job.setReducerClass(DistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }    
}