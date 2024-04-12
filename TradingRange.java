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
 * TradingRange class calculates the trading range for a particular stock
 */
public class TradingRange {
    /**
     * Mapper class is calculates the trading range and it is then emitted to the reducer
     */
    public static class RangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable tradingRange = new DoubleWritable();
        private Text date = new Text();
        /**
         * Map method iterates through all the entries in the dataset
         * @param key The key represents the byte offset of the input data line
         * @param values The values are the contents of the line
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when reading from the file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            //checks if there are enough columns to process the data
            if (columns.length >= 5 && !columns[0].equals("Date")) {
                //date, high and low data values are parsed
                String dateColumn = columns[0];
                String highColumn = columns[2];
                String lowColumn = columns[3];
                double highValue = Double.parseDouble(highColumn);
                double lowValue = Double.parseDouble(lowColumn);
                //calculating the trading range
                double range = highValue - lowValue;
                date.set(dateColumn);
                tradingRange.set(range);
                //emitting date and trading range
                context.write(date, tradingRange);
            }
        }
    }  
     /**
     * Reducer class aggregates the data and emits it as output
     */
    public static class RangeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        /**
         * Reduce method iterates through all the key value pairs emitted from the mapper
         * @param key The key represents a date
         * @param values The values are a collection of trading range values
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when writing to the output file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String range = key.toString().replace("Trading Range for: ", "  ");
            for (DoubleWritable value : values) {
                //date and trading range are emitted as output in this format
                context.write(new Text ("Trading Range for: " + range), value);
            }      
        }   
    }
    /**
     * Main method holds the configuration setup
     * @param args args specifies the input and output paths
    */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "trading range calculation");
        job.setJarByClass(TradingRange.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(RangeMapper.class);
        job.setCombinerClass(RangeReducer.class);
        job.setReducerClass(RangeReducer.class);
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }    
}