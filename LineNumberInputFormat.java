import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
//Override the createRecordReader method to provide a custom RecordReader
public class LineNumberInputFormat extends FileInputFormat<LongWritable, Text> {
    //the code underneath was borrowed and modified from mainly 3 sources, as the conversion from ByteOffset to line number was not inherently important to solving problem 2
    //https://gist.github.com/dedunumax/96594b09d0f566e88ced
    //https://www.kamalsblog.com/2017/07/custom-n-line-record-reader-in-hadoop.html?m=1
    //https://stackoverflow.com/questions/15598537/how-to-set-custom-input-format-in-mapreduce
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // Return a new instance of LineNumberRecordReader when createRecordReader is called
        return new LineNumberRecordReader();
    }
    //Define the custom RecordReader class called LineNumberRecordReader
    public static class LineNumberRecordReader extends RecordReader<LongWritable, Text> {
        private LineRecordReader lineRecordReader;
        private long lineNumber;
        //Initialize the RecordReader
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
            lineNumber = 0;
        }
        //Move to the next key-value pair
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean hasNext = lineRecordReader.nextKeyValue();
            if (hasNext==true) {
                lineNumber++;
            }
            return hasNext;
        }
        //Get the current key (line number)
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(lineNumber);
        }
        //Get the current value (line content)
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return lineRecordReader.getCurrentValue();
        }
        //Get the progress of reading the input
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }
        //Close the RecordReader
        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }
}
