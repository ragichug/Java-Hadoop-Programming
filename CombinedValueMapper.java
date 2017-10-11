package org.apache.hadoop.examples;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import java.io.*;
public class CombinedValueMapper implements Writable{

private LongWritable PktLen;
private Text SrcMAC;
private Text DestMAC;
private Text SrcIp;
private Text DestIp;
private LongWritable TimeStamp;
public CombinedValueMapper()
{
PktLen = new LongWritable();
SrcMAC = new Text();
DestMAC =new Text();
SrcIp = new Text();
DestIp = new Text();
TimeStamp=new LongWritable();
}

public CombinedValueMapper(LongWritable PktLen,LongWritable TimeStamp,Text SrcMAC,Text DestMAC,Text SrcIp,Text DestIp)
{
this.PktLen=PktLen;
this.TimeStamp= TimeStamp;
this.SrcMAC=SrcMAC;
this.DestMAC=DestMAC;
this.SrcIp=SrcIp;
this.DestIp=DestIp;

}

public void set(LongWritable PktLen,LongWritable TimeStamp,Text SrcMAC,Text DestMAC,Text SrcIp,Text DestIp)
{

this.PktLen=PktLen;
this.TimeStamp= TimeStamp;
this.SrcMAC=SrcMAC;
this.DestMAC=DestMAC;
this.SrcIp=SrcIp;
this.DestIp=DestIp;
}
public void write(DataOutput out)throws IOException
{
PktLen.write(out);
TimeStamp.write(out);
SrcMAC.write(out);
DestMAC.write(out);
SrcIp.write(out);
DestIp.write(out);

}
public String toString()
{
return PktLen+","+TimeStamp+","+SrcMAC+","+DestMAC+","+SrcIp+","+DestIp;
}
public void readFields(DataInput in)throws IOException
{
PktLen.readFields(in);
TimeStamp.readFields(in);
SrcMAC.readFields(in);
DestMAC.readFields(in);
SrcIp.readFields(in);
DestIp.readFields(in);
}

public LongWritable getPktLen()
{
return PktLen;}

public Text getSrcMAC()
{return SrcMAC;}

public Text getDestMAC()
{
return DestMAC;}

public LongWritable getTimeStamp()
{
return TimeStamp;}

public Text getSrcIp()
{
return SrcIp;
}
public Text getDestIp()
{
return DestIp;
}
}
