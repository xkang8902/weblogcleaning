package cn.com.comleader.mr.weblog.visits;

import cn.com.comleader.mr.weblog.pageview.PageViewBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This is Description
 *
 * @author arc
 * @date 2018/10/17
 */
public class ClickStreamVisit {
    static class ClickStreamVisitMapper extends Mapper<LongWritable, Text, Text, PageViewBean> {
        Text k = new Text();
        PageViewBean v = new PageViewBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("&&");
            int step = Integer.parseInt(fields[5]);
            k.set(fields[0]);
            v.set(fields[0], fields[1], fields[2], fields[3], fields[4], step, fields[6], fields[7], fields[8], fields[9]);
            context.write(k, v);
        }
    }

    static class ClickStreamVisitReducer extends Reducer<Text, PageViewBean, NullWritable, VisitBean> {
        VisitBean v = new VisitBean();

        @Override
        protected void reduce(Text session, Iterable<PageViewBean> values, Context context) throws IOException, InterruptedException {
        List<PageViewBean> pvList = new ArrayList<PageViewBean>();
            for (PageViewBean pageViewBean : values) {
                PageViewBean pv = new PageViewBean();
                try {
                    BeanUtils.copyProperties(pv, pageViewBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                pvList.add(pv);
            }
            //按照第几步进行排序
            Collections.sort(pvList, new Comparator<PageViewBean>() {
                @Override
                public int compare(PageViewBean o1, PageViewBean o2) {
                    return o1.getStep() - o2.getStep();
                }
            });

            v.setSession(session.toString());
            v.setInTime(pvList.get(0).getTimestr());
            v.setOutTime(pvList.get(pvList.size() - 1).getTimestr());

            v.setInPage(pvList.get(0).getReferer());
            v.setOutPage(pvList.get(pvList.size() - 1).getReferer());

            v.setReferer(pvList.get(0).getReferer());
            v.setPageVisits(pvList.get(pvList.size() - 1).getStep());
            v.setRemote_addr(pvList.get(0).getRemote_addr());
            context.write(NullWritable.get(), v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClickStreamVisit.class);
        //设置map  和   reduce  的类
        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setReducerClass(ClickStreamVisitReducer.class);
        //设置mapper的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewBean.class);
        //设置reducer的输出
        job.setOutputKeyClass(NullWritable.get().getClass());
        job.setOutputValueClass(VisitBean.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/arc/Documents/bigdataprj/data/mr/output/pageviewoutput/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/arc/Documents/bigdataprj/data/mr/output/clickstreamoutput"));

        boolean res = job.waitForCompletion(true);
        System.exit(true == res ? 0 : 1);
    }
}
