package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import org.apache.hadoop.io.*;
public class CombinedKey implements WritableComparable{
private Text src_Ip;
private Text dest_Ip;
private Text src_Port;
private Text dest_Port;
//private Text src_Mac;
//private Text dest_Mac;
private Text protocol;
private Text first,second;
public CombinedKey() {
set(new Text(), new Text(),new Text(),new Text(),new Text());
}
public CombinedKey(String srcIp, String second) {
set(new Text(src_Ip),new Text(dest_Ip),new Text(src_Port),new Text(dest_Port),new Text(protocol));
}
public CombinedKey(Text src_Ip,Text dest_Ip,Text src_Port,Text dest_Port,Text protocol) {
set(src_Ip,dest_Ip,src_Port,dest_Port,protocol);
}
public void set(Text src_Ip,Text dest_Ip,Text src_Port,Text dest_Port, Text protocol) {
this.src_Ip = src_Ip;
this.dest_Ip = dest_Ip;
this.src_Port = src_Port;
this.dest_Port = dest_Port;
this.protocol = protocol;
}
public Text getSrcIp() {
return src_Ip;
}
public Text getDestIp() {
return dest_Ip;
}
public Text getSrcPort()
{
return src_Port;
}
public Text getDestPort()
{
return dest_Port;
}/*
public Text getSrcMac()
{
return src_Mac;
}
public Text getDestMac()
{
return dest_Mac;
}*/
public Text getProtocol()
{
return protocol;
}
@Override
public void write(DataOutput out) throws IOException {
src_Ip.write(out);
dest_Ip.write(out);
src_Port.write(out);
dest_Port.write(out);
//out.write(src_Mac);
//out.write(dest_Mac);
protocol.write(out);
}
public void readFields(DataInput in) throws IOException {
src_Ip.readFields(in);
dest_Ip.readFields(in);
src_Port.readFields(in);
dest_Port.readFields(in);
//src_Mac=in.read();
//dest_Mac=in.read();
protocol.readFields(in);
}/*
@Override
public int hashCode() {
return first.hashCode() * 163 + second.hashCode();
}*/
@Override
public boolean equals(Object o) {
if (o instanceof CombinedKey) {
CombinedKey tp = (CombinedKey) o;
if(src_Ip.equals(tp.src_Ip) && dest_Ip.equals(tp.dest_Ip) && src_Port.equals(tp.src_Port) && dest_Port.equals(tp.dest_Port)  && protocol.equals(tp.protocol))
return true;
else if(src_Ip.equals(tp.dest_Ip) && dest_Ip.equals(tp.src_Ip) && src_Port.equals(tp.dest_Port) && dest_Port.equals(tp.src_Port) && protocol.equals(tp.protocol))
return true;
else
return false;
}
return false;
}
@Override
public String toString() {
return src_Ip + "," + dest_Ip + "," +src_Port + "," +dest_Port+ "," + protocol;
}


public int compareTo(Object o) {
CombinedKey tp=(CombinedKey)o;
if((0==src_Ip.compareTo(tp.src_Ip)) && (0==dest_Ip.compareTo(tp.dest_Ip)) && (0==src_Port.compareTo(tp.src_Port)) && (0==dest_Port.compareTo(tp.dest_Port))  && (0==protocol.compareTo(tp.protocol)))
return 0;
else if ((0==src_Ip.compareTo(tp.dest_Ip)) && (0==dest_Ip.compareTo(tp.src_Ip)) && (0==src_Port.compareTo(tp.dest_Port)) && (0==dest_Port.compareTo(tp.src_Port))  && (0==protocol.compareTo(tp.protocol)))
return 0;
else return -1;
}

}

