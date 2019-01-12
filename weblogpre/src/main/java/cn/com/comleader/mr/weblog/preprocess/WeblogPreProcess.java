package cn.com.comleader.mr.weblog.preprocess;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * This is Description
 * @author arc
 * @date 2018/10/15
 */
public class WeblogPreProcess {
    static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //存储网站url的分类数据
        Set<String> pages = new HashSet<String>();
        Text k = new Text();
        NullWritable v = NullWritable.get();
        /**
         * 从外部配置文件中加载网站有用的url数据   存储到maptask内存中 实现对数据的过滤
         * 还可以通过配置文件的方式加载  等会实现
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WeblogBean weblogBean = WeblogParser.parser(line);
            if (weblogBean!=null) {
                //过滤js/图片/css等静态资源
                WeblogParser.filterStaticResource(weblogBean, pages);
                //if (!weblogBean.isValid()) return;
                k.set(weblogBean.toString());
                context.write(k,v);
            }

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WeblogPreProcess.class);
        job.setMapperClass(WeblogPreProcessMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("/Users/arc/Documents/bigdataprj/data/mr/input/access.log.fensi"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/arc/Documents/bigdataprj/data/mr/output/preoutput"));
        job.setNumReduceTasks(0);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
