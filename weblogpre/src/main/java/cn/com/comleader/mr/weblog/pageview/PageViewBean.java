package cn.com.comleader.mr.weblog.pageview;/**
 * Created by arc on 16/10/2018.
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is Description
 *
 * @author arc
 * @date 2018/10/16
 */
public class PageViewBean implements Writable {

    private String session;
    private String remote_addr;
    private String timestr;
    private String request;
    private int step;
    private String staylong;
    private String referer;
    private String useragent;
    private String bytes_send;
    private String status;

    public void set(String session,
                    String remote_addr,
                    String useragent,
                    String timestr,
                    String request,
                    int step,
                    String staylong,
                    String referer,
                    String bytes_send,
                    String status) {

        this.session = session;
        this.remote_addr = remote_addr;
        this.timestr = timestr;
        this.request = request;
        this.step = step;
        this.staylong = staylong;
        this.referer = referer;
        this.useragent = useragent;
        this.bytes_send = bytes_send;
        this.status = status;

    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getTimestr() {
        return timestr;
    }

    public void setTimestr(String timestr) {
        this.timestr = timestr;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getStaylong() {
        return staylong;
    }

    public void setStaylong(String staylong) {
        this.staylong = staylong;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUseragent() {
        return useragent;
    }

    public void setUseragent(String useragent) {
        this.useragent = useragent;
    }

    public String getBytes_send() {
        return bytes_send;
    }

    public void setBytes_send(String bytes_send) {
        this.bytes_send = bytes_send;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(session + "&&")
                .append(remote_addr + "&&")
                .append(useragent + "&&")
                .append(timestr + "&&")
                .append(request + "&&")
                .append(step + "&&")
                .append(staylong + "&&")
                .append(referer + "&&")
                .append(bytes_send + "&&")
                .append(status);

        return sb.toString();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(session);
        dataOutput.writeUTF(remote_addr);
        dataOutput.writeUTF(timestr);
        dataOutput.writeUTF(request);
        dataOutput.writeInt(step);
        dataOutput.writeUTF(staylong);
        dataOutput.writeUTF(referer);
        dataOutput.writeUTF(useragent);
        dataOutput.writeUTF(bytes_send);
        dataOutput.writeUTF(status);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.session = dataInput.readUTF();
        this.remote_addr = dataInput.readUTF();
        this.timestr = dataInput.readUTF();
        this.request = dataInput.readUTF();
        this.step = dataInput.readInt();
        this.staylong = dataInput.readUTF();
        this.referer = dataInput.readUTF();
        this.useragent = dataInput.readUTF();
        this.bytes_send = dataInput.readUTF();
        this.status = dataInput.readUTF();
    }
}
