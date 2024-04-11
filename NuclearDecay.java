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

//nuclear decay class that calculates basic order statistics for mass excess uncertainty values
public class NuclearDecay {
    //mapper extracts mass excess uncertainty values and emits key-value pairs
    public static class NuclearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable occurence = new DoubleWritable(1.0);
        @Override
        //map method processes each entry of the column "massExcessUncertainty"
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            //ensuring that there are enough columns in the dataset to emit the key-value pairs
            if (columns.length >= 11) {
                String massExcessColumn = columns[10];
                try {
                    //emit key-value pairs for each mass excess uncertainty value
                    double massExcessValue = Double.parseDouble(massExcessColumn); 
                    //the format for the key-value pairs emitted for the mode is different as the key represents the mass excess value and the value represents the occurence
                    context.write(new Text(massExcessColumn), occurence);
                    context.write(new Text("Mean: "), new DoubleWritable(massExcessValue));            
                    context.write(new Text("Minimum: "), new DoubleWritable(massExcessValue));             
                    context.write(new Text("Maximum: "), new DoubleWritable(massExcessValue));
                    context.write(new Text("Median: "), new DoubleWritable(massExcessValue));
                } catch (NumberFormatException e) {
                    //catch statement to deal with any problem regarding the parsing of massExcessColumn as a double
                    System.out.println("There was a problem with the value: " + "\\\"" + massExcessColumn + "\\\"");
                }
            }
        }
    }
    
    // Reducer class computes basic order statistics of mass excess uncertainty values
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
        //reduce method is used to aggregate and process the key-value pairs
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                double currentValue = value.get();
                //calculates the minimum
                if (key.toString().equals("Minimum: ") && currentValue < minimumValue) {
                    minimumValue = currentValue;
                }
                //calculates the maximum
                if (key.toString().equals("Maximum: ") && currentValue > maximumValue) {
                    maximumValue = currentValue;
                }
                //calculates sum and count for mean calculation
                if (key.toString().equals("Mean: ")) {
                    sum += currentValue;
                    count++;
                }
                //values are collected for median computation
                if (key.toString().equals("Median: ")) {
                    valuesList.add(currentValue);
                }
                //Hashmaps are used to collect the values and their occurences for mode calculation
                if (!(key.toString().equals("Median: ")) && !(key.toString().equals("Mean: ")) && !(key.toString().equals("Maximum: ")) && !(key.toString().equals("Minimum: "))) {
                    mode.put(key.toString(), mode.getOrDefault(key.toString(), 0.0) + currentValue);         
                }
            } 
            //mode calculation
            for (Map.Entry<String, Double> entry : mode.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxKey = entry.getKey();
                    maxCount = entry.getValue();
                }
            }      
        }
        //cleanup method is used to emit the final results after processing all the key-value pairs
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //compute and emit mean 
            if (count > 0) {
                double mean = sum / count;
                context.write(new Text("Mean: "), new DoubleWritable(mean));
            }
            //compute median  
            Collections.sort(valuesList);
            int size = valuesList.size();
            double median;
            if (size % 2 == 0) {
                median = (valuesList.get(size / 2 - 1) + valuesList.get(size / 2)) / 2.0;
            } else {
                median = valuesList.get(size / 2);
            }
            //emit key-value pairs for mode, median, minimum and maximum
            String modeOutputKey = maxKey.replace("Mode: ", "  ");
            context.write(new Text("Mode: " + modeOutputKey), new DoubleWritable(maxCount));  
            context.write(new Text("Median: "), new DoubleWritable(median));
            context.write(new Text("Minimum: "), new DoubleWritable(minimumValue));
            context.write(new Text("Maximum: "), new DoubleWritable(maximumValue));
        }
    }
    //hadoop job configuration are held in the main method
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
