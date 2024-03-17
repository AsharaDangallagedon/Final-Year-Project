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

public class PriceChange {
    public static class PriceChangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable priceChange = new DoubleWritable();
        private Text date = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 5 && !columns[0].equals("Date")) {
                String dateColumn = columns[0];
                String openColumn = columns[1];
                String closeColumn = columns[4];
                double openValue = Double.parseDouble(openColumn);
                double closeValue = Double.parseDouble(closeColumn);
                double change = openValue - closeValue;
                date.set(dateColumn);
                priceChange.set(change);
                context.write(date, priceChange);
            }
        }
    }  

    public static class PriceChangeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String change = key.toString().replace("Price Change for: ", "  ");
            for (DoubleWritable value : values) {
                context.write(new Text ("Price Change for: " + change), value);
            }      
        }   
    }
    
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