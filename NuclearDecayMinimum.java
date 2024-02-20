import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class NuclearDecayMinimum {
    public static class MinimumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable massExcessUncertainty = new DoubleWritable(99.0);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 11) {
                String massExcessColumn = columns[10];
                try {
                    double massExcess = Double.parseDouble(massExcessColumn);
                    if (massExcess < massExcessUncertainty.get()) {
                        massExcessUncertainty.set(massExcess);
                    }
                    context.write(new Text("Minimum: "), massExcessUncertainty);
                } catch (NumberFormatException e) {
                    System.out.println("There was a problem with the value: " + "\\\"" + massExcessColumn + "\\\"");
                }
            }
        }
    }

    public static class MinimumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable minimumUncertainty = new DoubleWritable(99.0);
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                if (value.get() < minimumUncertainty.get()) {
                    minimumUncertainty.set(value.get());
                }
            }
            context.write(key, minimumUncertainty);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "minimum calculation");
        job.setJarByClass(NuclearDecayMinimum.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(MinimumMapper.class);
        job.setCombinerClass(MinimumReducer.class);
        job.setReducerClass(MinimumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }       
}