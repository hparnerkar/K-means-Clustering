

import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.net.URI;
import java.util.StringTokenizer;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.JobContext;
import java.util.Scanner;
import java.util.Vector;
import java.io.InputStreamReader;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


class Point implements WritableComparable <Point>
{
	public Double x,y;
	public Point(double x, double y)
    {
        this.x=x;
        this.y=y;
    }
	public Point() {
  
    	
    }
	public void readFields(DataInput in) throws IOException 
	{
		x= in.readDouble();
		y= in.readDouble();
		
	}
	public void write(DataOutput out) throws IOException 
	{
		out.writeDouble(x);
		out.writeDouble(y);
		
	}
	public void set(double a,double b)
	{
		this.x= a;
		this.y= b;
	}
	public Double getx()
	{
		return x;
	}
	public Double gety()
	{
		return y;
	}

	public String toString() {
		return this.x+""+this.y;
		
	}
	public int compareTo(Point p) {
		 if(p.x == this.x && p.y == this.y)
		 {
			 return 0;
		 }
		 else
		 {
			 return 1;
		 }// TODO Auto-generated method stub
	
	}
}

public class KMeans 
{
	
	static Vector<Point> centroids = new Vector<Point>(100);
	
    public static class AvgMapper extends Mapper<Object,Text,Point,Point> 
    {
    	@Override
    	
    	public void setup(Context context) throws IOException, InterruptedException	
        {
        	
        	URI[] paths = context.getCacheFiles();
        	Configuration conf = context.getConfiguration();
        	FileSystem fs = FileSystem.get(conf);
        	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
        	//
        	
        	double a,b;
        	//double i = 0;
        	String Line;
        	try {
        	while((Line= reader.readLine()) != null)
        	{ String st[];
        		st=Line.split(",");
    			a= Double.parseDouble(st[0]);
    			b= Double.parseDouble(st[1]);
    			Point p = new Point(a,b);
    			//p.set(a,b);
    			centroids.addElement(p);
        		
        	}
        	}
        	finally {
        		reader.close();
        		
        	}
        } 
    	
    	public void map(Object Key, Text line, Context context ) throws IOException, InterruptedException
    	// string k jagah text
    	{ 
    		
    		 
    		//double tempx,tempy;   	
    		double dist=0;
    		double mindist=99.9f;
    		//Point c;
    		double j,k;
    		String l= line.toString();
    		String st2[]=l.split(",");
			j= Double.parseDouble(st2[0]);
			k= Double.parseDouble(st2[1]);
			Point p1 = new Point(j,k);
			//p1.set(j,k);
    		int centupdated=0;
    		int minindex=0;
    		Point p5=new Point();

    		for(Point p2 : centroids)
    		{ double m= p2.getx();
    		  double n= p2.gety();
    		dist = (j- m )*(j-m) + (k-n)*(k-n);
    		Math.sqrt(dist);
    		if ( dist < mindist ) 
    			{	
    			
    				  mindist = dist;
    				  minindex=centupdated;// keep track of the latest index
    				  p5=centroids.get(minindex);
    			}
    		else
    		{
    			try {
    			
    			context.write(p2,p1);
    			}
    		catch(IOException e)
    			{
    			e.printStackTrace();
    			}
    		
    		
    		}
    		centupdated++;
    		}
    		//WritableComparable<Point> p5= new WritableComparable<Point>(minindex);
    		
    		//DoubleWritable db= new DoubleWritable(p5);
    		try {
    		context.write(centroids.get(minindex),p1);
    		}
    		catch(IOException e){
    			System.out.println(e);
    			
    		}
    		
    	
    	
    	}

    }
    
    public static class AvgReducer extends Reducer<Point,Point,Text,Object> //text karna hai key
    { @Override
    	public void reduce( Point c,Iterable<Point> p1, Context context)throws IOException, InterruptedException
    	{
    		double count = 0; 
    		double sx = 0.0, sy = 0.0;
    		for (Point point : p1)
    		{
    			
    			count = count+1.0;  
    			sx += point.getx() ;
    			sy += point.gety() ;
    		}
    		
    		c.x = sx/count; 
    		c.y = sy/count; 
    		
    		try
    		{ Text t1= new Text(c.toString());
    			context.write(t1, NullWritable.get());	
    		}	
    		catch(IOException ex){
    		ex.printStackTrace();
    		}

    	}
    }
    

    public static void main ( String[] args ) throws Exception 
    {	//String datafile = args[0];
    	//String centroidfile= args[1];
    	Configuration conf= new 	Configuration();
    	Job job = Job.getInstance();
        job.setJobName("KmeansJob");
        job.addCacheFile(new URI(args[1]));
        job.setJarByClass(KMeans.class);
        job.setOutputKeyClass(Text.class);//reducer me text daalo
        job.setOutputValueClass(Object.class);// null writable daalo  //object?
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Point.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

    
    
    }
}

