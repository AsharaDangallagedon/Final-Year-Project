import java.io.IOException;
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
/**
 * @author Ashara Dangallage don
 * VolumeChange class for calculating the volume rate of change
 */
public class VolumeChange {
    /**
     * Mapper class is used to calculate the volume rate of change and emits it to the reducer
     * @returns A text is returned as a key which represents the Date
     * @returns A DoubleWritable is returned as a value which represents the Volume Rate of Change
     */
    public static class VolumeChangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable volumeChange = new DoubleWritable();
        private Text date = new Text();
        //variable used to keep track of the previous volume
        private double previousVolume = 0.0;
        /**
         * Map method extract volume data from dataset
         * @param key The key represents the byte offset of the input data line
         * @param values The values are the contents of the line
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when reading from the file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            //checks if the input line contains the necessary amount of columns to actually process data
            if (columns.length >= 7 && !columns[0].equals("Date")) {
                String dateColumn = columns[0];
                //calculates the volume rate of change if previous volume is available
                double volume = Double.parseDouble(columns[6]);
                if (previousVolume != 0.0) {
                    //I used the VROC equation
                    double volumechange = (((volume - previousVolume) / previousVolume) * 100.0);
                    date.set(dateColumn);
                    volumeChange.set(volumechange);
                    context.write(date, volumeChange);
                }
                //update previous volume
                previousVolume = volume;
            }
        }
    }  
    /**
     * Reducer class aggregates all the key-value pairs emitted by the mapper
     * @returns A text is returned as a key which represents the Date
     * @returns A DoubleWritable is returned as a value which represents the Volume Rate of Change
     */
    public static class VolumeChangeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        /**
         * Reduce method iterates through all the key-vlaue pairs
         * @param key The key represents a date
         * @param values The values are a collection of volume rate of change values
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when writing to the output file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //I had a problem regarding the duplication of code so I replaced it with a black space
            String volumeRateChange = key.toString().replace("Volume Rate of Change for:", "  ");
            for (DoubleWritable value : values) {
                //the data and the volume rate of change are emitted as ouput
                context.write(new Text ("Volume Rate of Change for:" + volumeRateChange), value);
            }      
        }   
    }
    /**
     * Main method holds the necessary configurations in order to run the hadoop job
     * @param args args specifies the input and output paths
    */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Volume rate of change calculation");
        job.setJarByClass(VolumeChange.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(VolumeChangeMapper.class);
        job.setCombinerClass(VolumeChangeReducer.class);
        job.setReducerClass(VolumeChangeReducer.class);
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }    
}