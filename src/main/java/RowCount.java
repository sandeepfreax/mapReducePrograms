import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
* This program can be used for - select count(1) from 'table'
* 1. Mapper function
* 2. reducer function
* 3. Job Configuration
* */

public class RowCount extends Configured implements Tool {

    public static void main(String[] st) throws Exception {
        int exitCode = ToolRunner.run(new RowCount(), st);
        System.exit(exitCode);
    }

    public int run(String[] st) throws Exception {
        Job job = Job.getInstance(getConf(), "Word count using built in mappers and reducers");
        job.setJarByClass(getClass());
        FileInputFormat.setInputPaths(job, new Path(st[0]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(RecordMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(NoKeyRecordCountReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(st[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
