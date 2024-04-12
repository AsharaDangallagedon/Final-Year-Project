import java.io.IOException;
import java.util.ArrayList;
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
/**
 * @author Ashara Dangallage don
 * RSI class is used to calculate the rsi indicator for the dataset
 */
public class RSI {
    /**
     * Mapper class extracts price data (open and close) in order to calculate the RSI
     * @returns A text is returned as a key which represents the Date
     * @returns A DoubleWritable is returned as a value which represents the RSI
     */
    public static class RSIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        //array list keeps track of the closing prices of the financial data
        private ArrayList<Double> closePrices = new ArrayList<>();
        /**
         * Map method processes each line of the input data
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
            //it also makes sure it is not a header
            if (columns.length >= 7 && !columns[0].equals("Date")) {
                double closePrice = Double.parseDouble(columns[4]);
                closePrices.add(closePrice);
                //RSI period in this case is 30 days
                if (closePrices.size() > 30) {
                    double upwards = 0.0;
                    double downwards = 0.0;
                    //iterate through the values in order to calculate the upward and downward movements
                    for (int i = 1; i < closePrices.size(); i++) {
                        double difference = closePrices.get(i) - closePrices.get(i - 1);
                        if (difference > 0) {
                            upwards += difference;
                        } else {
                            downwards += 0 - difference;
                        }
                    }
                    //calculates the relative strength (RS) which is part of the whole RSI formula
                    double averageUpwards = upwards/30;
                    double averageDownwards = downwards/30;
                    double relativestrenght;
                    if (averageDownwards == 0) { 
                        relativestrenght = 99.0;
                    } else {
                        relativestrenght = averageUpwards/averageDownwards;
                    }
                    //use RSI formula to calculate the RSI
                    double rsi = 100-(100/(1 + relativestrenght));
                    //date and RSI are emitted from the mapper
                    context.write(new Text(columns[0]), new DoubleWritable(rsi));
                    //remove the oldest closing price
                    closePrices.remove(0);
                }
            }
        }
    }
    /**
     * Reducer class aggregates the key value pairs emitted by the mapper
     * @returns A text is returned as a key which represents the Date
     * @returns A DoubleWritable is returned as a value which represents the RSI
     */
    public static class RSIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        /**
         * Reduce method processes each key-value pair emitted by the mapper
         * @param key The key represents a date
         * @param values The values are a collection of RSI values
         * @param context The context object is required for Mapper/Reducer classes to interact with the Hadoop system
         * @throws IOException This error occurs in case of issues when writing to the output file
         * @throws InterruptedException This error occurs in case interruptions occur during the execution of the Hadoop job
         */
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //Had a problem regarding the duplication of this specific string so I replaced it with blank space
            String RSImetric = key.toString().replace("RSI for:", "  ");
            //dates and RSI values are outputted from the reducer
            for (DoubleWritable value : values) {
                context.write(new Text ("RSI for:" + RSImetric), value);
            }
        }
    }
    /**
     * Main method holds the configurations necessary for the hadoop job
     * @param args args specifies the input and output paths
    */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RSI Calculation");
        job.setJarByClass(RSI.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(RSIMapper.class);
        job.setCombinerClass(RSIReducer.class);
        job.setReducerClass(RSIReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
