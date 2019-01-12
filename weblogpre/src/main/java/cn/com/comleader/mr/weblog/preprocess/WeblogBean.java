package cn.com.comleader.mr.weblog.preprocess;/**
 * Created by arc on 15/10/2018.
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is Description
 *
 * @author arc
 * @date 2018/10/15
 */
public class WeblogBean implements Writable {
    private boolean valid = true;   //判断用户是否合法
    private String remote_addr;     //客户端ip
    private String remote_user; //客户端用户名称
    private String time_local;  //访问时间
    private String request; //请求的url和http协议
    private String status;  //请求的状态
    private String body_bytes_sent; //发送给客户端文件主体的大小
    private String http_referer;    //记录从哪个页面链接访问过来的
    private String http_user_agent; //浏览器的相关信息

    public void set(boolean valid,
                    String remote_addr,
                    String remote_user,
                    String time_local,
                    String request,
                    String status,
                    String body_bytes_sent,
                    String http_referer,
                    String http_user_agent) {
        this.valid = valid;
        this.remote_addr = remote_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(valid)
                .append("&&").append(remote_addr)
                .append("&&").append(remote_user)
                .append("&&").append(time_local)
                .append("&&").append(request)
                .append("&&").append(status)
                .append("&&").append(body_bytes_sent)
                .append("&&").append(http_referer)
                .append("&&").append(http_user_agent);
        return sb.toString();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(valid);
        dataOutput.writeUTF(null == remote_addr ? "" : remote_addr);
        dataOutput.writeUTF(null == remote_user ? "" : remote_user);
        dataOutput.writeUTF(null == time_local ? "" : time_local);
        dataOutput.writeUTF(null == status ? "" : status);
        dataOutput.writeUTF(null == request ? "" : request);
        dataOutput.writeUTF(null == body_bytes_sent ? "" : body_bytes_sent);
        dataOutput.writeUTF(null == http_referer ? "" : http_referer);
        dataOutput.writeUTF(null == http_user_agent ? "" : http_user_agent);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        valid = dataInput.readBoolean();
        remote_addr = dataInput.readUTF();
        remote_user = dataInput.readUTF();
        time_local = dataInput.readUTF();
        status = dataInput.readUTF();
        request = dataInput.readUTF();
        body_bytes_sent = dataInput.readUTF();
        http_referer = dataInput.readUTF();
        http_user_agent = dataInput.readUTF();
    }
}
