import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class OutdatedAccountChecker {

    public static class AccessLogsMapper
            extends Mapper<Object, Text, Text, Text>{
        private Map<String, String> dataMap;
        private Map<String, Integer> accessMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            dataMap = new HashMap<>();
            accessMap = new HashMap<>();
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] items = line.split(",");
                dataMap.put(items[0], items[1]);
            }
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String pageID = fields[1];
            Integer current = accessMap.get(pageID);
            Integer access = Integer.parseInt(fields[4]) / (24 * 60);
            if (current == null) {
                // accessMap.put(pageID, 1);
                accessMap.put(pageID, access);
            } else {
                if (access < current) {
                    accessMap.put(pageID, access);
                }
                // accessMap.put(pageID, current + 1);
            }
            System.out.println(pageID + " " + accessMap.get(pageID));
            context.write(new Text(pageID), new Text(Integer.toString(accessMap.get(pageID))));
        }

        /*@Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            List<String> pageIDs = new ArrayList<>(accessMap.keySet());
            List<Integer> counts = new ArrayList<>(accessMap.values());
            List<Integer> sorted = new ArrayList<>(counts);
            Collections.sort(sorted, Collections.reverseOrder());
            for (int i = 0; i < 10; i++) {
                Integer count = sorted.get(i);
                int index = counts.indexOf(count);
                String pageID = pageIDs.get(index);
                String[] data = dataMap.get(pageID);
                counts.remove(index);
                pageIDs.remove(index);
                context.write(new Text(pageID), new Text(count.toString() + "," + data[0] + "," + data[1]));
            }
        }*/
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "outdated checker");
        job.setJarByClass(OutdatedAccountChecker.class);
        job.setMapperClass(AccessLogsMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///D:/DS503/FaceInPage.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "outdated checker");
        job.setJarByClass(OutdatedAccountChecker.class);
        job.setMapperClass(AccessLogsMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///D:/DS503/FaceInPage.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
