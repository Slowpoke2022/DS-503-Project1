import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountryCounter {

    public static class FaceInPageMapper1
            extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String country = fields[3];
            context.write(new Text(country), new IntWritable(1));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class FaceInPageMapper2
            extends Mapper<Object, Text, Text, IntWritable>{
        private Map<String, Integer> countMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            countMap = new HashMap<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String country = fields[3];
            Integer count = countMap.get(country);
            if (count == null) {
                countMap.put(country, 1);
            } else {
                countMap.put(country, count + 1);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String country : countMap.keySet()) {
                Integer count = countMap.get(country);
                context.write(new Text(country), new IntWritable(count));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country count");
        job.setJarByClass(CountryCounter.class);
        if (args[2].equals("optimized")) {
            job.setMapperClass(FaceInPageMapper2.class);
        } else {
            job.setMapperClass(FaceInPageMapper1.class);
            job.setReducerClass(IntSumReducer.class);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
        Job job = Job.getInstance(conf, "country count");
        job.setJarByClass(CountryCounter.class);
        if (args[2].equals("optimized")) {
            job.setMapperClass(FaceInPageMapper2.class);
        } else {
            job.setMapperClass(FaceInPageMapper1.class);
            job.setReducerClass(IntSumReducer.class);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }
}
