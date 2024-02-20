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

public class NuclearDecayMaximum {
    public static class MaximumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable massExcessUncertainty = new DoubleWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 11) {
                String massExcessColumn = columns[10];
                try {
                    double massExcess = Double.parseDouble(massExcessColumn);
                    if (massExcess > massExcessUncertainty.get()) {
                        massExcessUncertainty.set(massExcess);
                    }
                    context.write(new Text("Maximum: "), massExcessUncertainty);
                } catch (NumberFormatException e) {
                    System.out.println("There was a problem with the value: " + "\\\"" + massExcessColumn + "\\\"");
                }
            }
        }
    }

    public static class MaximumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable maximumUncertainty = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                if (value.get() > maximumUncertainty.get()) {
                    maximumUncertainty.set(value.get());
                }
            }
            context.write(key, maximumUncertainty);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maximum calculation");
        job.setJarByClass(NuclearDecayMaximum.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(MaximumMapper.class);
        job.setCombinerClass(MaximumReducer.class);
        job.setReducerClass(MaximumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }       
}