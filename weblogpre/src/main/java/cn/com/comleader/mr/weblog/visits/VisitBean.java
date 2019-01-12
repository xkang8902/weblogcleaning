package cn.com.comleader.mr.weblog.visits;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is Description
 *
 * @author arc
 * @date 2018/10/17
 */
public class VisitBean implements Writable {
    private String session;
    private String remote_addr;
    private String inTime;
    private String outTime;
    private String inPage;
    private String outPage;
    private String referer;
    private int pageVisits;

    void set(String session,
             String remote_addr,
             String inTime,
             String outTime,
             String inPage,
             String outPage,
             String referer,
             int pageVisits) {
        this.session = session;
        this.remote_addr = remote_addr;
        this.inTime = inTime;
        this.outTime = outTime;
        this.inPage = inPage;
        this.outPage = outPage;
        this.referer = referer;
        this.pageVisits = pageVisits;
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

    public String getInTime() {
        return inTime;
    }

    public void setInTime(String inTime) {
        this.inTime = inTime;
    }

    public String getOutTime() {
        return outTime;
    }

    public void setOutTime(String outTime) {
        this.outTime = outTime;
    }

    public String getInPage() {
        return inPage;
    }

    public void setInPage(String inPage) {
        this.inPage = inPage;
    }

    public String getOutPage() {
        return outPage;
    }

    public void setOutPage(String outPage) {
        this.outPage = outPage;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public int getPageVisits() {
        return pageVisits;
    }

    public void setPageVisits(int pageVisits) {
        this.pageVisits = pageVisits;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(session + "&&")
                .append(remote_addr + "&&")
                .append(inTime + "&&")
                .append(outTime + "&&")
                .append(inPage + "&&")
                .append(outPage + "&&")
                .append(referer + "&&")
                .append(pageVisits + "&&");
        return sb.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(session);
        dataOutput.writeUTF(remote_addr);
        dataOutput.writeUTF(inTime);
        dataOutput.writeUTF(outTime);
        dataOutput.writeUTF(inPage);
        dataOutput.writeUTF(outPage);
        dataOutput.writeUTF(referer);
        dataOutput.writeInt(pageVisits);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.session = dataInput.readUTF();
        this.remote_addr = dataInput.readUTF();
        this.inTime = dataInput.readUTF();
        this.outTime = dataInput.readUTF();
        this.inPage = dataInput.readUTF();
        this.outPage = dataInput.readUTF();
        this.referer = dataInput.readUTF();
        this.pageVisits = dataInput.readInt();
    }
}
