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

public class RSI {
    public static class RSIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private ArrayList<Double> closePrices = new ArrayList<>();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 7 && !columns[0].equals("Date")) {
                double closePrice = Double.parseDouble(columns[4]);
                closePrices.add(closePrice);
                if (closePrices.size() > 14) {
                    double upwards = 0.0;
                    double downwards = 0.0;
                    for (int i = 1; i < closePrices.size(); i++) {
                        double difference = closePrices.get(i) - closePrices.get(i - 1);
                        if (difference > 0) {
                            upwards += difference;
                        } else {
                            downwards += 0 - difference;
                        }
                    }
                    double averageUpwards = upwards/14;
                    double averageDownwards = downwards/14;
                    double relativestrenght;
                    if (averageDownwards == 0) { 
                        relativestrenght = 99.0;
                    } else {
                        relativestrenght = averageUpwards/averageDownwards;
                    }
                    double rsi = 100-(100/(1 + relativestrenght));
                    context.write(new Text(columns[0]), new DoubleWritable(rsi));
                    closePrices.remove(0);
                }
            }
        }
    }

    public static class RSIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String RSImetric = key.toString().replace("RSI for:", "  ");
            for (DoubleWritable value : values) {
                context.write(new Text ("RSI for:" + RSImetric), value);
            }
        }
    }

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
