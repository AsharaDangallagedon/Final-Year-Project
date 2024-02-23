import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NuclearDecayMedian {
    public static class MedianMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable massExcessUncertainty = new DoubleWritable();  
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 11) {
                String massExcessColumn = columns[10];
                try {
                    massExcessUncertainty.set(Double.parseDouble(massExcessColumn));
                    context.write(new Text("Median: "), massExcessUncertainty);
                } catch (NumberFormatException e) {
                    System.out.println("There was a problem with the value: " + "\\\"" + massExcessColumn + "\\\"");
                }
            }
        }
    }

    public static class MedianReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private ArrayList<Double> valuesList = new ArrayList<>();
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                valuesList.add(value.get());
            }
            Collections.sort(valuesList);
            int size = valuesList.size();
            double median;
            if (size % 2 == 0) {
                median = (valuesList.get(size/2 + 1) + valuesList.get(size/2))/2.0;
            } else {
                median = valuesList.get(size/2);
            }
            context.write(new Text("Median"), new DoubleWritable(median));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "median calculation");
        job.setJarByClass(NuclearDecayMedian.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(MedianMapper.class);
        job.setReducerClass(MedianReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }       
}
