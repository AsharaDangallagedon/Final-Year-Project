import java.io.IOException;
import javax.naming.Context;
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

public class NuclearDecayMode {
    public static class ModeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

    public static class ModeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable cumulativecount = new IntWritable();
        private Text mode = new Text();
        private int maxCount;
    
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            if (sum > maxCount) {
                maxCount = sum;
                mode.set(key);
                cumulativecount.set(sum); 
            }
        }
    
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(mode, cumulativecount); 
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mode calculation");
        job.setJarByClass(NuclearDecayMode.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(ModeMapper.class);
        job.setCombinerClass(ModeReducer.class);
        job.setReducerClass(ModeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }            
}