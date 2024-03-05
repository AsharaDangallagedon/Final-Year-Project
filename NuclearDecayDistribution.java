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

public class NuclearDecayDistribution {
    public static class DistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text massExcessUncertainty = new Text();
        private IntWritable occurence = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text currentline, Context context) throws IOException, InterruptedException {
            String[] columns = currentline.toString().split(",");
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

    public static class DistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable mode = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (!key.toString().equals("AP") && !key.toString().equals("massExcessUncertainty")) {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                mode.set(sum);
                context.write(key, mode);
            }
        }
    }
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