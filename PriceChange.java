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
//class PriceChange is used to calculate the price change for a particular stock
public class PriceChange {
    public static class PriceChangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable priceChange = new DoubleWritable();
        private Text date = new Text();
        //map methods iterates through all the entries in the dataset
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            //checks if there are enough columns to process the data
            if (columns.length >= 5 && !columns[0].equals("Date")) {
                //date, open and close data values are parsed
                String dateColumn = columns[0];
                String openColumn = columns[1];
                String closeColumn = columns[4];
                double openValue = Double.parseDouble(openColumn);
                double closeValue = Double.parseDouble(closeColumn);
                //calculating the price change
                double change = openValue - closeValue;
                date.set(dateColumn);
                priceChange.set(change);
                //date and price change are emitted by the mapper
                context.write(date, priceChange);
            }
        }
    }  
    //reducer class aggregates the data and emits them as output
    public static class PriceChangeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override 
        //reduce method iterates through all the key value pairs emitted from the mapper
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String change = key.toString().replace("Price Change for: ", "  ");
            for (DoubleWritable value : values) {
                //date and price change are emitted as output in this format
                context.write(new Text ("Price Change for: " + change), value);
            }      
        }   
    }
    //main method holds the configuration setup
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "price change calculation");
        job.setJarByClass(PriceChange.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(PriceChangeMapper.class);
        job.setCombinerClass(PriceChangeReducer.class);
        job.setReducerClass(PriceChangeReducer.class);
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }    
}