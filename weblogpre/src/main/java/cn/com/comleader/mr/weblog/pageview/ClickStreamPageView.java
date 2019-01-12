package cn.com.comleader.mr.weblog.pageview;

import cn.com.comleader.mr.weblog.preprocess.WeblogBean;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This is Description
 *
 * @author arc
 * @date 2018/10/16
 */
public class ClickStreamPageView {
    static class ClickStreamMapper extends Mapper<LongWritable, Text, Text, WeblogBean> {

        Text k = new Text();
        WeblogBean v = new WeblogBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("&&");
            if (fields.length < 9) return;
            v.set("true".equals(fields[0]) ? true : false, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);
            if (v.isValid()) {
                k.set(v.getRemote_addr());
                context.write(k, v);
            }
        }
    }

    static class ClickStreamReducer extends Reducer<Text, WeblogBean, NullWritable, PageViewBean> {

        PageViewBean v = new PageViewBean();

        @Override
        protected void reduce(Text key, Iterable<WeblogBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<WeblogBean> beans = new ArrayList<WeblogBean>();
            //先将用户的数据拿出来进行排序  通过新建数组和元素的方式进行  这样形参是引用变量的话可以保护他不被修改.
            for (WeblogBean bean : values) {
                WeblogBean weblogBean = new WeblogBean();
                try {
                    BeanUtils.copyProperties(weblogBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                beans.add(weblogBean);
            }
            //按时间进行排序
            Collections.sort(beans, new Comparator<WeblogBean>() {
                @Override
                public int compare(WeblogBean o1, WeblogBean o2) {

                    Date d1 = toDate(o1.getTime_local());
                    Date d2 = toDate(o2.getTime_local());
                    if (d1 == null || d2 == null) {
                        return 0;
                    }
                    return d1.compareTo(d2);
                }
            });

            int step = 1;
            String session = UUID.randomUUID().toString();
            for (int i = 0; i < beans.size(); i++) {
                WeblogBean bean = beans.get(i);
                //仅有一条数据
                if (1 == beans.size()) {
                    v.set(session,
                            key.toString(),
                            beans.get(0).getHttp_user_agent(),
                            beans.get(0).getTime_local(),
                            beans.get(0).getRequest(),
                            step,
                            60 + "",     //默认停留时间是60秒
                            beans.get(0).getHttp_referer(),
                            beans.get(0).getBody_bytes_sent(),
                            beans.get(0).getStatus()
                    );
                    context.write(NullWritable.get(), v);
                    break;
                }
                //不止有一条数据  第一条数据留在比较完毕后再输出.
                if (i == 0) {
                    continue;
                }
                Long timediff = timeDiff(bean.getTime_local(), beans.get(i - 1).getTime_local());
                if (timediff < 30 * 60 * 60) {
                    v.set(session,
                            key.toString(),
                            beans.get(i - 1).getHttp_user_agent(),
                            beans.get(i - 1).getTime_local(),
                            beans.get(i - 1).getRequest(),
                            step,
                            timediff / 1000 + "",     //默认停留时间是60秒
                            beans.get(i - 1).getHttp_referer(),
                            beans.get(i - 1).getBody_bytes_sent(),
                            beans.get(i - 1).getStatus()
                    );
                    context.write(NullWritable.get(), v);
                    step++;
                } else {
                    v.set(session,
                            key.toString(),
                            beans.get(i - 1).getHttp_user_agent(),
                            beans.get(i - 1).getTime_local(),
                            beans.get(i - 1).getRequest(),
                            step,
                            timediff / 1000 + "",     //默认停留时间是60秒
                            beans.get(i - 1).getHttp_referer(),
                            beans.get(i - 1).getBody_bytes_sent(),
                            beans.get(i - 1).getStatus()
                    );
                    context.write(NullWritable.get(), v);
                    //重置step和session
                    step = 1;
                    session = UUID.randomUUID().toString();
                }
                //最后一条数据
                if (i == beans.size() - 1) {
                    v.set(session,
                            key.toString(),
                            beans.get(i - 1).getHttp_user_agent(),
                            beans.get(i - 1).getTime_local(),
                            beans.get(i - 1).getRequest(),
                            step,
                            timediff / 1000 + "",     //默认停留时间是60秒
                            beans.get(i - 1).getHttp_referer(),
                            beans.get(i - 1).getBody_bytes_sent(),
                            beans.get(i - 1).getStatus()
                    );
                    context.write(NullWritable.get(), v);
                }

            }
        }

        private Long timeDiff(String date, String date1) {
            //gettime得到时间的毫秒值
            return toDate(date).getTime() - toDate(date1).getTime();
        }

        private Date toDate(String time_local) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                return sdf.parse(time_local);
            } catch (ParseException e) {
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ClickStreamPageView.class);

        job.setMapperClass(ClickStreamMapper.class);
        job.setReducerClass(ClickStreamReducer.class);
        //设置map的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WeblogBean.class);
        //设置reduce的输出
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/arc/Documents/bigdataprj/data/mr/output/preoutput"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/arc/Documents/bigdataprj/data/mr/output/pageviewoutput"));

        boolean res = job.waitForCompletion(true);
        System.exit(true == res ? 0 : 1);

    }
}
