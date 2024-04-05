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

public class TradingRange {
    public static class RangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable tradingRange = new DoubleWritable();
        private Text date = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 5 && !columns[0].equals("Date")) {
                String dateColumn = columns[0];
                String highColumn = columns[2];
                String lowColumn = columns[3];
                double highValue = Double.parseDouble(highColumn);
                double lowValue = Double.parseDouble(lowColumn);
                double range = highValue - lowValue;
                date.set(dateColumn);
                tradingRange.set(range);
                context.write(date, tradingRange);
            }
        }
    }  

    public static class RangeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String range = key.toString().replace("Trading Range for: ", "  ");
            for (DoubleWritable value : values) {
                context.write(new Text ("Trading Range for: " + range), value);
            }      
        }   
    }
    
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