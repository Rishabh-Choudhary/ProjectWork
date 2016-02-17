/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author rishabh
 */
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Floyd {

  public static class FloydMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
      StringTokenizer itr = new StringTokenizer(value.toString());
      if(value.toString().trim().length()!=0)
	{
	      int i=Integer.parseInt(itr.nextToken());
	      int j=0;
	      String rowI=new String("");
	      String rowIK=new String("");
	      while (itr.hasMoreTokens()) {
		String ij=new String(itr.nextToken());        
		rowI=rowI+" "+ij;
		if(j==k)
		{
			for(int ik=0;ik<n;ik++)
			{
				rowIK=rowIK+" "+ij;
			}	
		}	
		j++;
	      }
	      context.write(new Text(""+i),new Text(rowI));
		System.out.println("key "+i+"::value "+rowI);
	      context.write(new Text(i+"k"),new Text(rowIK));
		System.out.println("key "+i+"k::value "+rowIK);
	      if(i==k)
		{
			for(int kj=0;kj<n;kj++)
			{
				context.write(new Text(kj+"k"),new Text(rowI));
				System.out.println("key "+kj+"k::value "+rowI);
			}
		}
	}
    }
  }

  public static class FloydReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	String keyString=key.toString();
	if(keyString.contains("k"))
	{
		context.write(new Text(keyString.substring(0,keyString.length()-1)),getSum(values));
	}
	else
	{
		context.write(key,getMin(values));
	}
    }
  
    public Text getSum(Iterable<Text> values)
    {
	String sum=null;      	
	for(Text val:values)
	{
		if(sum==null) sum=new String(val.toString());
		else
		{	
			
			StringTokenizer valitr = new StringTokenizer(val.toString());
			StringTokenizer sumitr = new StringTokenizer(sum);
			String tempsum=new String("");
			while (valitr.hasMoreTokens() && sumitr.hasMoreTokens()) {
        			tempsum=tempsum+" "+(Integer.parseInt(valitr.nextToken())+Integer.parseInt(sumitr.nextToken()));
        		}
			sum=new String(tempsum);
		}
	}	
	return new Text(sum);
    }

    public Text getMin(Iterable<Text> values)
    {
	String min=null;      	
	for(Text val:values)
	{
		if(min==null) min=new String(val.toString());
		else
		{	
			StringTokenizer valitr = new StringTokenizer(val.toString());
			StringTokenizer minitr = new StringTokenizer(min);
			String tempmin=new String("");      	
			while (valitr.hasMoreTokens() && minitr.hasMoreTokens()) {
				int valval=Integer.parseInt(valitr.nextToken());
				int minval=Integer.parseInt(minitr.nextToken());
				tempmin=tempmin+" "+(valval<minval?valval:minval);
			}
			min=new String(tempmin);
		}
	}
	return new Text(min);
    }

  }

  public static void main(String[] args) throws Exception {
	for(int k=0;k<Integer.parseInt(args[0]);k++){
		Configuration conf = new Configuration();
		conf.set("k",k+"");
		conf.set("n",args[0]);	    	
		Job job = Job.getInstance(conf, "floyd");
		job.setJarByClass(Floyd.class);
	    	job.setMapperClass(FloydMapper.class);
	    	job.setCombinerClass(FloydReducer.class);
	    	job.setReducerClass(FloydReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
		String inputprefix=new String(k==0?"":""+k+"");
		String outputprefix=new String(""+(k+1)+"");
	    	FileInputFormat.addInputPath(job, new Path(args[1]+inputprefix));
	    	FileOutputFormat.setOutputPath(job, new Path(args[1]+outputprefix));
	    	job.waitForCompletion(true);
	}
  }
}
