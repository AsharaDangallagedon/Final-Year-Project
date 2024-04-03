import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class NuclearDecay {
    public static class NuclearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable occurence = new DoubleWritable(1.0);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 11) {
                String massExcessColumn = columns[10];
                try {
                    double massExcessValue = Double.parseDouble(massExcessColumn); 
                    context.write(new Text(massExcessColumn), occurence);
                    context.write(new Text("Mean: "), new DoubleWritable(massExcessValue));            
                    context.write(new Text("Minimum: "), new DoubleWritable(massExcessValue));             
                    context.write(new Text("Maximum: "), new DoubleWritable(massExcessValue));
                    context.write(new Text("Median: "), new DoubleWritable(massExcessValue));
                } catch (NumberFormatException e) {
                    System.out.println("There was a problem with the value: " + "\\\"" + massExcessColumn + "\\\"");
                }
            }
        }
    }
    
    
    public static class NuclearReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private double minimumValue = 99.0;
        private double maximumValue;
        private double sum = 0.0;
        private int count = 0;
        private List<Double> valuesList = new ArrayList<>();
        private Map<String, Double> mode = new HashMap<>();
        private double maxCount;
        String maxKey = null;
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                double currentValue = value.get();
                if (key.toString().equals("Minimum: ") && currentValue < minimumValue) {
                    minimumValue = currentValue;
                }
                if (key.toString().equals("Maximum: ") && currentValue > maximumValue) {
                    maximumValue = currentValue;
                }
                if (key.toString().equals("Mean: ")) {
                    sum += currentValue;
                    count++;
                }
                if (key.toString().equals("Median: ")) {
                    valuesList.add(currentValue);
                }
                if (!(key.toString().equals("Median: ")) && !(key.toString().equals("Mean: ")) && !(key.toString().equals("Maximum: ")) && !(key.toString().equals("Minimum: "))) {
                    mode.put(key.toString(), mode.getOrDefault(key.toString(), 0.0) + currentValue);         
                }
            } 
            for (Map.Entry<String, Double> entry : mode.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxKey = entry.getKey();
                    maxCount = entry.getValue();
                }
            }      
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (count > 0) {
                double mean = sum / count;
                context.write(new Text("Mean: "), new DoubleWritable(mean));
            }
            Collections.sort(valuesList);
            int size = valuesList.size();
            double median;
            if (size % 2 == 0) {
                median = (valuesList.get(size / 2 - 1) + valuesList.get(size / 2)) / 2.0;
            } else {
                median = valuesList.get(size / 2);
            }
            String modeOutputKey = maxKey.replace("Mode: ", "  ");
            context.write(new Text("Mode: " + modeOutputKey), new DoubleWritable(maxCount));  
            context.write(new Text("Median: "), new DoubleWritable(median));
            context.write(new Text("Minimum: "), new DoubleWritable(minimumValue));
            context.write(new Text("Maximum: "), new DoubleWritable(maximumValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job combinedJob = Job.getInstance(conf, "combined calculation");
        combinedJob.setJarByClass(NuclearDecay.class);
        combinedJob.setInputFormatClass(TextInputFormat.class); 
        combinedJob.setMapperClass(NuclearMapper.class);
        combinedJob.setCombinerClass(NuclearReducer.class);
        combinedJob.setReducerClass(NuclearReducer.class);
        combinedJob.setOutputKeyClass(Text.class);
        combinedJob.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(combinedJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(combinedJob, new Path(args[1]));
        int combinedSuccess = combinedJob.waitForCompletion(true) ? 0 : 1;
    }       
}
