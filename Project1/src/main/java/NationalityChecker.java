import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NationalityChecker {

    public static class FaceInPageMapper
            extends Mapper<Object, Text, Text, Text>{

        private String filter;

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            filter = conf.get("filter");
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String name = fields[1];
            String nationality = fields[2];
            String hobby = fields[4];
            if (nationality.equals(filter)) {
                context.write(new Text(name), new Text(hobby));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("filter", args[2]);
        Job job = Job.getInstance(conf, "nationality check");
        job.setJarByClass(NationalityChecker.class);
        job.setMapperClass(FaceInPageMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("filter", args[2]);
        Job job = Job.getInstance(conf, "nationality check");
        job.setJarByClass(NationalityChecker.class);
        job.setMapperClass(FaceInPageMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }
}