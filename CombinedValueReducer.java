package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
public class CombinedValueReducer implements Writable
{

private long tbt;
private long maxPktLen;
private long minPktLen;
private long totOutgoing;
private long totIncoming;
private ArrayList<Long> dpl=new ArrayList<Long>();
private ArrayList<Long> allPktLen=new ArrayList<Long>();
private long []arrivalTimeArray = new long[5];
private Text srcMac;
private Text destMac;
private Text srcIp;
private Text destIp;

 

private long currentTimeStamp;
private int count = 1;
public CombinedValueReducer(long pktLen, long timeStamp, Text srcMac,Text destMac, Text srcIp,Text destIp)
{

this.srcMac=srcMac;
this.destMac=destMac;
this.srcIp=srcIp;
this.destIp=destIp;
this.maxPktLen=pktLen;
this.minPktLen=pktLen;

totOutgoing = 1;
totIncoming = 0;


this.currentTimeStamp = timeStamp;
dpl.add(pktLen);
allPktLen.add(pktLen);
tbt = pktLen;
arrivalTimeArray[0]=currentTimeStamp;
}

public String toString()
{

return getFirstPacketLength()+","+getSecondPacketLength()+","+getThirdPacketLength()+","+getFourthPacketLength()+","+getFifthPacketLength()+","+(totIncoming+totOutgoing)+","+tbt+","+getAPL()+","+maxPktLen+","+minPktLen+","+getDPL()+","+getDuration()+","+getFirstInterArrival()+","+getSecondInterArrival()+","+getThirdInterArrival()+","+getFourthInterArrival()+","+getMeanInterArrival()+","+totOutgoing+","+totIncoming+","+getPv()+","+getLabel();
}

public long getFirstPacketLength()
{
return allPktLen.get(0);
}
public long getSecondPacketLength()
{
	if(allPktLen.size()>1)
	return allPktLen.get(1);
	else
	return -1;
}

public long getThirdPacketLength()
{
	if(allPktLen.size()>2)
	return allPktLen.get(2);
	else
	return -1;
}	

public long getFourthPacketLength()
{
	if(allPktLen.size()>3)
	return allPktLen.get(3);
	else
	return -1;
}	

public long getFifthPacketLength()
{
	if(allPktLen.size()>4)
	return allPktLen.get(4);
	else
	return -1;
}

public long getFirstInterArrival()
{
	if(arrivalTimeArray[1]!=0)
	return arrivalTimeArray[1]-arrivalTimeArray[0];
	else
	return -1;
}
public long getSecondInterArrival()
{

	if(arrivalTimeArray[2]!=0)
	return arrivalTimeArray[2]-arrivalTimeArray[1];
	else
	return -1;
}
public long getThirdInterArrival()
{
	if(arrivalTimeArray[3]!=0)
	return arrivalTimeArray[3]-arrivalTimeArray[2];
	else
	return -1;
}
public long getFourthInterArrival()
{
	if(arrivalTimeArray[4]!=0)
	return arrivalTimeArray[4]-arrivalTimeArray[3];
	else
	return -1;
}
public double getPv()
{
		int i;
		double sum = 0; 
		double apl = getAPL();
		for(i=0; i<allPktLen.size();i++)
		{
			sum += Math.pow(allPktLen.get(i)-apl,2);
		}

		return sum /((double) (totIncoming+totOutgoing));
}


public String getLabel()
{

	if((srcIp.toString().equals("172.16.2.11")&& srcMac.toString().equals("bb:bb:bb:bb:bb:bb"))||(destIp.toString().equals("172.16.2.11")&&destMac.toString().equals("bb:bb:bb:bb:bb:bb")))

			return "UDP(Storm)";

		else if((srcIp.toString().equals("172.16.0.2")&&srcMac.toString().equals("aa:aa:aa:aa:aa:aa"))||(destIp.toString().equals("172.16.0.2")&&destMac.toString().equals("aa:aa:aa:aa:aa:aa")))

			return "SMTP Spam(Waledac)";

		else if((srcIp.toString().equals("172.16.0.11")&&srcMac.toString().equals("aa:aa:aa:aa:aa:aa"))||(destIp.toString().equals("172.16.0.11")&&destMac.toString().equals("aa:aa:aa:aa:aa:aa")))

			return "SMTP Spam(Waledac)";

		else if((srcIp.toString().equals("172.16.0.12")&&srcMac.toString().equals("aa:aa:aa:aa:aa:aa"))||(destIp.toString().equals("172.16.0.12")&&destMac.toString().equals("aa:aa:aa:aa:aa:aa")))

			return "SMTP Spam(Storm)";

		else if((srcIp.toString().equals("172.16.2.12")&&srcMac.toString().equals("cc:cc:cc:cc:cc:cc"))||(destIp.toString().equals("172.16.2.12")&&destMac.toString().equals("cc:cc:cc:cc:cc:cc")))

			return "Zeus";

		else if((srcIp.toString().equals("172.16.2.12")&&srcMac.toString().equals("cc:cc:cc:dd:dd:dd"))||(destIp.toString().equals("172.16.2.12")&&destMac.toString().equals("cc:cc:cc:dd:dd:dd")))

			return "Zeus(C & C)";

		else

			return "Non-Malicious";

	}
public long getDuration()
{
return currentTimeStamp - arrivalTimeArray[0];
}
public void write(DataOutput out)throws IOException
{
out.writeLong(tbt);
out.writeLong(maxPktLen);
out.writeLong(minPktLen);

//tbt.write(out);
//maxPktLen.write(out);
//minPktLen.write(out);
srcMac.write(out);
destMac.write(out);
srcIp.write(out);
destIp.write(out);
out.writeLong(currentTimeStamp);
//currentTimeStamp.write(out);
out.writeLong(totIncoming);
out.writeLong(totOutgoing);
}
public void add(long pktLen,long timeStamp,Text srcIp,Text destIp)
{
if(srcIp.equals(this.srcIp) && destIp.equals(this.destIp))
totOutgoing++;
else 
totIncoming++;

if(pktLen>maxPktLen)
maxPktLen=pktLen;
if(pktLen<minPktLen)
minPktLen=pktLen;
currentTimeStamp=timeStamp;
if(!dpl.contains(pktLen))
dpl.add(pktLen);
allPktLen.add(pktLen);
tbt +=pktLen;
		if(count <= 4)
		{	
			arrivalTimeArray[count++] = timeStamp;
		}
}

public double getDPL()
{
   return (double)(dpl.size()/(double)(totIncoming+totOutgoing));
}

public double getAPL()
{
  return  (tbt/(double)(totIncoming+totOutgoing));

}

public double getMeanInterArrival()
{
    return ((currentTimeStamp-arrivalTimeArray[0])/(double)(totIncoming+totOutgoing));
}

public void readFields(DataInput in)throws IOException
{
tbt=in.readLong();
maxPktLen=in.readLong();
minPktLen=in.readLong();

//tbt.readFields(in);
//maxPktLen.readFields(in);
//minPktLen.readFields(in);
srcMac.readFields(in);
destMac.readFields(in);
srcIp.readFields(in);
destIp.readFields(in);
currentTimeStamp=in.readLong();
totIncoming=in.readLong();
totOutgoing=in.readLong();
}

}
