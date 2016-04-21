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
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Kmeans {

  public static class StageOneMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0];
			if(id.startsWith("D"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
				for(int i=0;i<k;i++)
				{
					String[] clusterDatum=keyValue[1].trim().split("\\|");
					context.write(new Text(id+"C"+i+":"),new Text(clusterDatum[1]));
				}
			}
			else if(id.startsWith("C"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1]));
				for(int i=0;i<n;i++)
				{
					String[] clusterInfo=keyValue[1].trim().split("\\|");
					context.write(new Text("D"+i+id+":"),new Text(clusterInfo[2]));
				}
			}
		}
    }
  }

  public static class StageOneReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.matches("D[0-9]+C[0-9]+:"))
		{
			String clusterId=keyString.substring(keyString.indexOf("C"),keyString.length()-1);
			Iterator dimValItr=values.iterator();
			String dimValueOne=((Text)dimValItr.next()).toString().trim();
			if(dimValItr.hasNext())
			{
				String[] dimValueOneArray=dimValueOne.split(" ");
				String[] dimValueTwoArray=((Text)dimValItr.next()).toString().trim().split(" ");
				context.write(key,new Text(clusterId+"|"+ValueProcessor.calcDistSquare(dim,dimValueOneArray,dimValueTwoArray)));
			}
			else
			{
				context.write(key,new Text(dimValueOne));
			}
		}
		else
		{
			Iterator dimValItr=values.iterator();
			context.write(key,(Text)dimValItr.next());
		}
    }
  
  }

  public static class StageTwoMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
      int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			if(id.matches("D[0-9]+C[0-9]+"))
			{
				String datumRowId=id.substring(0,id.indexOf("C"));
				context.write(new Text(datumRowId+"One:"),new Text(keyValue[1].trim()));
			}
			else
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
		}
    }
  }

  public static class StageTwoReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.matches("D[0-9]+One:"))
		{
			context.write(key,ValueProcessor.getMinDistCluster(values));
		}
		else
		{
			Iterator dimValItr=values.iterator();
			context.write(key,(Text)dimValItr.next());
		}
    }
  
  }

  
  public static class StageThreeMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			String[] valArray=keyValue[1].trim().split("\\|");
			if(id.matches("D[0-9]+One"))
			{
				String clusterId=valArray[0];
				String datumRowId=id.replace("One","");
				context.write(new Text(datumRowId+":"),new Text(clusterId+"| "));
				context.write(new Text(clusterId+":"),new Text("1|"+datumRowId+"| "));
			}
			else if(id.startsWith("D"))
			{
				context.write(new Text(id+":"),new Text(" |"+valArray[1]));
			}
			else if(id.startsWith("C"))
			{
				context.write(new Text(id+":"),new Text("0| |"+valArray[2]));
			}
		}
    }
  }
  
  
  public static class StageThreeReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.startsWith("D"))
		{
			String[] clusterDatum=null;
			for(Text val:values)
			{
				if(clusterDatum==null) 							  
					clusterDatum=val.toString().split("\\|");
				else
				{
					String[] valArray=val.toString().split("\\|");
					if(valArray[0].length()>clusterDatum[0].length())
						clusterDatum[0]=valArray[0];
					if(valArray[1].length()>clusterDatum[1].length())
						clusterDatum[1]=valArray[1];
				}
			}
			context.write(key,new Text(clusterDatum[0]+"|"+clusterDatum[1]));
		}
		else if(keyString.startsWith("C"))
		{
			String[] clusterInfo=null;
			for(Text val:values)
			{
				if(clusterInfo==null) 							  
					clusterInfo=val.toString().split("\\|");
				else
				{
					String[] valArray=val.toString().split("\\|");					
					clusterInfo[0]=(Integer.parseInt(clusterInfo[0])+Integer.parseInt(valArray[0]))+"";
					clusterInfo[1]=clusterInfo[1].trim()+" "+valArray[1].trim();
					if(Integer.parseInt(valArray[0])==0)
						clusterInfo[2]=valArray[2];
				}
			}
			context.write(key,new Text(clusterInfo[0]+"|"+clusterInfo[1].trim()+"|"+clusterInfo[2]));
		}
		
    }
  
  }
  
  public static class StageFourMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			String[] valArray=keyValue[1].trim().split("\\|");
			if(id.startsWith("D"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
				context.write(new Text(id+valArray[0]+":"),new Text(valArray[1]));
			}
			else if(id.startsWith("C"))
			{
				if(Integer.parseInt(valArray[0])!=0)
				{
					context.write(new Text(id+":"),new Text(keyValue[1].trim()));
					String[] datumRowIdList=valArray[1].trim().split(" ");
					for(int i=0;i<datumRowIdList.length;i++)
					{
						String multiplier="";
						for(int j=0;j<dim;j++)
							multiplier=multiplier+" "+(1.0/Double.parseDouble(valArray[0]));
						context.write(new Text(datumRowIdList[i]+id+":"),new Text(multiplier.trim()));
					}
				}
			}
		}
    }
  }

  
  public static class StageFourReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.matches("D[0-9]+C[0-9]+:"))
		{
			Iterator valItr=values.iterator();
			String multiple=((Text)valItr.next()).toString().trim();
			if(valItr.hasNext())
			{
				String[] varArray=((Text)valItr.next()).toString().trim().split(" ");
				String[] mulArray=multiple.trim().split(" ");
				multiple="";
				for(int i=0;i<dim;i++)
					multiple=multiple+" "+(Double.parseDouble(mulArray[i])*Double.parseDouble(varArray[i]));	
				context.write(key,new Text(multiple.trim()));
			}
			else
				context.write(key,new Text(multiple));
		}
		else 
		{
			Iterator valItr=values.iterator();
			context.write(key,(Text)valItr.next());
		}
    }
  }
  
    public static class StageFiveMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
      int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			if(id.matches("D[0-9]+C[0-9]+"))
			{
				String clusterId=id.substring(id.indexOf("C"),id.length());
				context.write(new Text(clusterId+"One:"),new Text(keyValue[1].trim()));
			}
			else
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
		}
    }
  }

  
  public static class StageFiveReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.matches("C[0-9]+One:"))
		{
			Iterator valItr=values.iterator();
			String sum=null;
			for(Text val:values)
			{
				if(sum==null) sum=val.toString().trim();
				else
				{
					String[] varArray=val.toString().trim().split(" ");
					String[] sumArray=sum.trim().split(" ");
					sum="";
					for(int i=0;i<dim;i++)
						sum=sum+" "+(Double.parseDouble(sumArray[i])+Double.parseDouble(varArray[i]));	
				}				
			}
			context.write(key,new Text(sum.trim()));
		}
		else 
		{
			Iterator valItr=values.iterator();
			context.write(key,(Text)valItr.next());
		}
    }
  }

  public static class StageSixMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			if(id.matches("C[0-9]+One"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
			}
			else if(id.startsWith("C"))
			{
				String[] valArray=keyValue[1].trim().split("\\|");
				context.write(new Text(id+"One:"),new Text(valArray[2]));
			}
		}
    }
  }
  
  public static class StageSixReducer
       extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		double threshold=Double.parseDouble(conf.get("threshold"));
		String keyString=key.toString();
		if(keyString.matches("C[0-9]+One:"))
		{
			Iterator valItr=values.iterator();
			String valOne=((Text)valItr.next()).toString().trim();
			if(valItr.hasNext())
			{
				String[] valTwoArray=((Text)valItr.next()).toString().trim().split(" ");
				String[] valOneArray=valOne.split(" ");
				double tmp=ValueProcessor.calcDistSquare(dim,valOneArray,valTwoArray);
				if(tmp>threshold)
				{	context.write(key,new Text("0"));}
				else
				{	context.write(key,new Text("1"));}
			}
			else
				context.write(key,new Text(valOne));
		}
    }
  }
  
  public static class StageSevenMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			context.write(new Text("stop_iteration:"),new Text(keyValue[1].trim()));
		}
    }
  }
  
  public static class StageSevenReducer
       extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		double threshold=Double.parseDouble(conf.get("threshold"));
		String keyString=key.toString();
		if (keyString.equals("stop_iteration:"))
		{
			int multiple=1;
			for(Text val:values)
				multiple*=Integer.parseInt(val.toString());
			context.write(key,new Text(multiple+""));
		}
    }
  }

  public static class StageEightMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			if(id.startsWith("D"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
			}
			else if(id.matches("C[0-9]+One"))
			{
				context.write(new Text(id.replace("One","")+":"),new Text("0| |"+keyValue[1].trim()));
			}
			else if(id.startsWith("C"))
			{
				String[] valArray=keyValue[1].trim().split("\\|");
				context.write(new Text(id+":"),new Text(valArray[0]+"|"+valArray[1]+"| "));
			}
		}
    }
  }
  
  public static class StageEightReducer
       extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.startsWith("C"))
		{
			String[] clusterInfo=null;
			for(Text val:values)
			{
				if(clusterInfo==null) 							  
					clusterInfo=val.toString().split("\\|");
				else
				{
					String[] valArray=val.toString().split("\\|");					
					clusterInfo[0]=(Integer.parseInt(clusterInfo[0])+Integer.parseInt(valArray[0]))+"";
					clusterInfo[1]=clusterInfo[1]+" "+valArray[1];
					if(Integer.parseInt(valArray[0])==0)
						clusterInfo[2]=valArray[2];
				}
			}
			context.write(key,new Text(clusterInfo[0]+"|"+clusterInfo[1].trim()+"|"+clusterInfo[2]));
		}
		else if (keyString.startsWith("D"))
		{
			Iterator valItr=values.iterator();
			context.write(key,(Text)valItr.next());
		}
    }
  }
  
   public static class StageNineMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			String[] valArray=keyValue[1].trim().split("\\|");
			if(id.startsWith("D"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
				context.write(new Text(id+valArray[0]+":"),new Text(valArray[1]));
			}
			else if(id.startsWith("C"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
				String[] datumRowIdArray=valArray[1].trim().split(" ");
				for(String datumRowId:datumRowIdArray)
					context.write(new Text(datumRowId+id+":"),new Text(valArray[2]));
			}
		}
    }
  }
  
    public static class StageNineReducer
       extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.matches("D[0-9]+C[0-9]+:"))
		{
			Iterator valItr=values.iterator();
			String valOne=((Text)valItr.next()).toString().trim();
			if(valItr.hasNext())
			{
				String[] valTwoArray=((Text)valItr.next()).toString().trim().split(" ");
				String[] valOneArray=valOne.split(" ");
				double tmp=ValueProcessor.calcDistSquare(dim,valOneArray,valTwoArray);
				context.write(key,new Text(tmp+""));
			}
			else
				context.write(key,new Text(valOne));
		}
		else 
		{
			Iterator valItr=values.iterator();
			context.write(key,(Text)valItr.next());
		}
    }
  }

   public static class StageTenMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			if(id.matches("D[0-9]+C[0-9]+"))
			{
				String clusterId=id.substring(id.indexOf("C"),id.length());
				context.write(new Text(clusterId+"Radius:"),new Text(keyValue[1].trim()));
			}
			else
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
		}
    }
  }
  
    public static class StageTenReducer
       extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if (keyString.matches("C[0-9]+Radius:"))
		{
			double maxRadius=0.0;
			for(Text val:values)
				if(Double.parseDouble(val.toString())>maxRadius)
					maxRadius=Double.parseDouble(val.toString());
			context.write(key,new Text(maxRadius+""));
		}
		else 
		{
			Iterator valItr=values.iterator();
			context.write(key,(Text)valItr.next());
		}
    }
  }
  
  public static class StageElevenMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf=context.getConfiguration();
      int k=Integer.parseInt(conf.get("k"));
      int n=Integer.parseInt(conf.get("n"));
	  int dim=Integer.parseInt(conf.get("dim"));
      if(value.toString().trim().length()!=0)
		{
			String[] keyValue=value.toString().split(":");
			String id=keyValue[0].trim();
			String[] valArray=keyValue[1].trim().split("\\|");
			if(id.startsWith("D"))
			{
				context.write(new Text(id+":"),new Text(keyValue[1].trim()));
			}
			else if(id.matches("C[0-9]+Radius"))
			{
				context.write(new Text(id.replace("Radius","")+":"),new Text(Math.sqrt(Double.parseDouble(keyValue[1].trim()))+"|0| | "));
			}
			else if(id.startsWith("C"))
			{
				context.write(new Text(id+":"),new Text("0|"+keyValue[1].trim()));
			}
		}
    }
  }
  
    public static class StageElevenReducer
       extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	Configuration conf=context.getConfiguration();
		int k=Integer.parseInt(conf.get("k"));
		int n=Integer.parseInt(conf.get("n"));
		int dim=Integer.parseInt(conf.get("dim"));
		String keyString=key.toString();
		if(keyString.startsWith("C"))
		{
			String[] clusterInfo=null;
			for(Text val:values)
			{
				if(clusterInfo==null) 							  
					clusterInfo=val.toString().split("\\|");
				else
				{
					String[] valArray=val.toString().split("\\|");					
					clusterInfo[1]=(Integer.parseInt(clusterInfo[1])+Integer.parseInt(valArray[1]))+"";
					clusterInfo[2]=clusterInfo[2]+" "+valArray[2];
					if(Integer.parseInt(valArray[1])!=0)
						clusterInfo[3]=valArray[3];
					if(Integer.parseInt(valArray[1])==0)
						clusterInfo[0]=valArray[0];
				}
			}
			context.write(key,new Text(clusterInfo[0]+"|"+clusterInfo[1].trim()+"|"+clusterInfo[2]+"|"+clusterInfo[3]));
		}
		else 
		{
			Iterator valItr=values.iterator();
			context.write(key,(Text)valItr.next());
		}
    }
  }
  
  public static class ValueProcessor{
  
	public static double calcDistSquare(int dim,String[] dimValueOneArray,String[] dimValueTwoArray)
	{
		double tmp,dist=0.0;
		for(int i=0;i<dim;i++)
		{
			tmp=Double.parseDouble(dimValueOneArray[i])-Double.parseDouble(dimValueTwoArray[i]);
			dist+=tmp*tmp;
		}
		return dist;
	}
  
	public static Text getMinDistCluster(Iterable<Text> values)
	{
		String minDistCluster=null;
		for(Text val:values)
		{
			if(minDistCluster==null)
				minDistCluster=val.toString();
			else
			{
				String[] valArray=val.toString().split("\\|");
				String[] minDistClusterArray=minDistCluster.split("\\|");
				if(Double.parseDouble(valArray[1])<Double.parseDouble(minDistClusterArray[1]))
					minDistCluster=val.toString();
			}
		}
		return new Text(minDistCluster);
	}
  
  }

  public static void main(String[] args) throws Exception {
		String n=args[0];		
		String dim=args[1];		
		String k=args[2];
		String iterationLimit=args[3];
		String threshold=args[4];
		String inputFile=args[5];
		int stopIteration=1;
		int i;
		String stopCheckFile="StageSevenOutput";
		Configuration conf = new Configuration();
		conf.set("k",k);
		conf.set("n",n);
		conf.set("dim",dim);
		conf.set("threshold",threshold);
		Job job;
		FileSystem fs = FileSystem.get(conf);
		Path inputFilePath=new Path(inputFile);
		Path StageOneOutput=new Path("StageOneOutput");
		Path StageTwoOutput=new Path("StageTwoOutput");
		Path StageThreeOutput=new Path("StageThreeOutput");
		Path StageFourOutput=new Path("StageFourOutput");
		Path StageFiveOutput=new Path("StageFiveOutput");
		Path StageSixOutput=new Path("StageSixOutput");
		Path StageSevenOutput=new Path(stopCheckFile);
		Path StageEightOutput=new Path("StageEightOutput");
		Path StageNineOutput=new Path("StageNineOutput");
		Path StageTenOutput=new Path("StageTenOutput");
		Path StageElevenOutput=new Path("StageElevenOutput");		
	for(i=0;i<Integer.parseInt(iterationLimit);i++){
		try{
			fs.delete(StageOneOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageOne");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageOneMapper.class);
	    	job.setCombinerClass(StageOneReducer.class);
	    	job.setReducerClass(StageOneReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, inputFilePath);
	    	FileOutputFormat.setOutputPath(job, StageOneOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageTwoOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageTwo");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageTwoMapper.class);
	    	job.setCombinerClass(StageTwoReducer.class);
	    	job.setReducerClass(StageTwoReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageOneOutput);
	    	FileOutputFormat.setOutputPath(job, StageTwoOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageThreeOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageThree");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageThreeMapper.class);
	    	job.setCombinerClass(StageThreeReducer.class);
	    	job.setReducerClass(StageThreeReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageTwoOutput);
	    	FileOutputFormat.setOutputPath(job, StageThreeOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageFourOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageFour");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageFourMapper.class);
	    	job.setCombinerClass(StageFourReducer.class);
	    	job.setReducerClass(StageFourReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageThreeOutput);
	    	FileOutputFormat.setOutputPath(job, StageFourOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageFiveOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageFive");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageFiveMapper.class);
	    	job.setCombinerClass(StageFiveReducer.class);
	    	job.setReducerClass(StageFiveReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageFourOutput);
	    	FileOutputFormat.setOutputPath(job, StageFiveOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageSixOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageSix");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageSixMapper.class);
	    	job.setCombinerClass(StageSixReducer.class);
	    	job.setReducerClass(StageSixReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageFiveOutput);
	    	FileOutputFormat.setOutputPath(job, StageSixOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageSevenOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageSeven");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageSevenMapper.class);
	    	job.setCombinerClass(StageSevenReducer.class);
	    	job.setReducerClass(StageSevenReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageSixOutput);
	    	FileOutputFormat.setOutputPath(job, StageSevenOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageEightOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageEight");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageEightMapper.class);
	    	job.setCombinerClass(StageEightReducer.class);
	    	job.setReducerClass(StageEightReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageFiveOutput);
	    	FileOutputFormat.setOutputPath(job, StageEightOutput);
	    	job.waitForCompletion(true);
		try{
			Path pt=new Path(stopCheckFile+"/part-r-00000");
	    		
	    		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line=br.readLine();
			String[] keyValue=line.split(":");
			stopIteration=Integer.parseInt(keyValue[1].trim());			
			if(stopIteration!=0) break;
			else
				inputFilePath=StageEightOutput;
		}
		catch(Exception e){
			break;
				}
		}
		try{
			fs.delete(StageNineOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageNine");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageNineMapper.class);
	    	job.setCombinerClass(StageNineReducer.class);
	    	job.setReducerClass(StageNineReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageEightOutput);
	    	FileOutputFormat.setOutputPath(job, StageNineOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageTenOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageTen");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageTenMapper.class);
	    	job.setCombinerClass(StageTenReducer.class);
	    	job.setReducerClass(StageTenReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageNineOutput);
	    	FileOutputFormat.setOutputPath(job, StageTenOutput);
	    	job.waitForCompletion(true);
		try{
			fs.delete(StageElevenOutput,true);
		}
		catch(Exception e){}
		job = Job.getInstance(conf, "KmeansStageEleven");
		job.setJarByClass(Kmeans.class);
	    	job.setMapperClass(StageElevenMapper.class);
	    	job.setCombinerClass(StageElevenReducer.class);
	    	job.setReducerClass(StageElevenReducer.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	FileInputFormat.addInputPath(job, StageTenOutput);
	    	FileOutputFormat.setOutputPath(job, StageElevenOutput);
	    	job.waitForCompletion(true);
		System.out.println("Iterations completed: "+i);
  }
}
