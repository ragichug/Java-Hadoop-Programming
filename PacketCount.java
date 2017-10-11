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
import org.apache.hadoop.io.LongWritable;
public class PacketCount {

  public static class PacketMapper 
       extends Mapper<Object, Text, CombinedKey, CombinedValueMapper>{
   
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String timestamp = null, srcIp = null, destIp = null, srcPort = null, destPort = null, srcMac = null, destMac = null,protocol = null, packetLen = null;
      CombinedValueMapper cvm=null;
      CombinedKey ckey=null;
	String line = value.toString();
      String[] allLines = line.split("\\n");
      
for(int i=0;i<allLines.length;i++)
      {
      StringTokenizer itr = new StringTokenizer(allLines[i],",");

        
        timestamp = itr.nextToken();
        srcIp=itr.nextToken();
        destIp=itr.nextToken();
        srcPort=itr.nextToken();
        destPort=itr.nextToken();
	srcMac = itr.nextToken();
	destMac = itr.nextToken();
        protocol=itr.nextToken();
        packetLen=itr.nextToken();
       
       cvm = new CombinedValueMapper(new LongWritable(Long.parseLong(packetLen)),new LongWritable(Long.parseLong(timestamp)),new Text(srcMac),new Text(destMac),new Text(srcIp),new Text(destIp));
      ckey = new CombinedKey(new Text(srcIp),new Text(destIp),new Text(srcPort),new Text(destPort),new Text(protocol));  
      context.write(ckey,cvm);   
 
}   
}
  }
  
  public static class PacketReducer 
       extends Reducer<CombinedKey,CombinedValueMapper,CombinedKey,CombinedValueReducer> {
  private CombinedValueReducer cvd;

    public void reduce(CombinedKey key, Iterable<CombinedValueMapper> values, 
                       Context context) throws IOException, InterruptedException {

        long TPC=0L;
	long TBT=0L; 
	long maxPkt=0L;
	long minPkt=Long.MAX_VALUE;
        double DPL;
	int count = 0;
      for (CombinedValueMapper val : values) {
       if(count == 0)
	{
		cvd = new CombinedValueReducer(val.getPktLen().get(),val.getTimeStamp().get(),val.getSrcMAC(), val.getDestMAC(),val.getSrcIp(), val.getDestIp()); 
		count = 1;
	}
	else
		cvd.add(val.getPktLen().get(),val.getTimeStamp().get(),val.getSrcIp(),val.getDestIp());
       
      }
      
      context.write(key, cvd);
    }
  }

  public static void main(String[] args) throws Exception {
/*
    Configuration conf = new Configuration();
/* String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "packet count");
    job.setJarByClass(PacketCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(CombinedKey.class);
    job.setOutputValueClass(IntWritable.class);
    //job.setMapOutputKeyClass();
/*    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
*/

Job job=new Job();
job.setJarByClass(PacketCount.class);
job.setJobName("packet count");
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1])); 
    job.setMapperClass(PacketMapper.class);
   // job.setCombinerClass(PacketReducer.class);
    job.setReducerClass(PacketReducer.class);
    job.setOutputKeyClass(CombinedKey.class);
    job.setMapOutputKeyClass(CombinedKey.class);
   job.setMapOutputValueClass(CombinedValueMapper.class);
    job.setOutputValueClass(CombinedValueReducer.class);
   
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}










