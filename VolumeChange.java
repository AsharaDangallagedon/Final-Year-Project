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

public class VolumeChange {
    public static class VolumeChangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private DoubleWritable volumeChange = new DoubleWritable();
        private Text date = new Text();
        private double previousVolume = 0.0;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 7 && !columns[0].equals("Date")) {
                String dateColumn = columns[0];
                double volume = Double.parseDouble(columns[6]);
                if (previousVolume != 0.0) {
                    double volumechange = (((volume - previousVolume) / previousVolume) * 100.0);
                    date.set(dateColumn);
                    volumeChange.set(volumechange);
                    context.write(date, volumeChange);
                }
                previousVolume = volume;
            }
        }
    }  

    public static class VolumeChangeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String volumeRateChange = key.toString().replace("Volume Rate of Change for:", "  ");
            for (DoubleWritable value : values) {
                context.write(new Text ("Volume Rate of Change for:" + volumeRateChange), value);
            }      
        }   
    }
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