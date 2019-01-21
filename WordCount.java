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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private static IntWritable year;
    private Text type = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();

      
      String line = value.toString();
      String[] linearray;
      String[] gerenarray;
      linearray = line.split(";");//To Split the line using ; as 
      gerenarray = linearray[4].split(",");//To split the genre
      
      
        
      
        int year1;
      for(int i=0;i<gerenarray.length;i++)
      {
          try{
          year1=Integer.parseInt(linearray[3]);
        }catch(Exception e)
        {
          continue;
        }
          if(year1>=2000)
          {
              year=new IntWritable(year1);
              type.set(gerenarray[i]);
              context.write(type,year);
          }
      }
    


      //StringTokenizer itr = new StringTokenizer(value.toString());
      //while (itr.hasMoreTokens()) {
      //  word.set(itr.nextToken());
      //  context.write(word, one);
      //}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int[] count=new int[19];
      for(int i=0;i<19;i++)
      {
        //count[i]=0;
      }
      int y;
      for(IntWritable val : values)
      {
          y=val.get();
          System.out.println("The key is"+key+"the values are" +y);
          switch(y)
          {

            case 2000:count[0]=count[0]+1;System.out.println("Entered in 2000");break;
            case 2001:count[1]=count[1]+1;break;
            case 2002:count[2]=count[2]+1;break;
            case 2003:count[3]=count[3]+1;break;
            case 2004:count[4]=count[4]+1;break;
            case 2005:count[5]=count[5]+1;break;
            case 2006:count[6]=count[6]+1;break;
            case 2007:count[7]=count[7]+1;break;
            case 2008:count[8]=count[8]+1;break;
            case 2009:count[9]=count[9]+1;break;
            case 2010:count[10]=count[10]+1;break;
            case 2011:count[11]=count[11]+1;break;
            case 2012:count[12]=count[12]+1;break;
            case 2013:count[13]=count[13]+1;break;
            case 2014:count[14]=count[14]+1;break;
            case 2015:count[15]=count[15]+1;break;
            case 2016:count[16]=count[16]+1;break;
            case 2017:count[17]=count[17]+1;break;
            case 2018:count[18]=count[18]+1;break;
            default :System.out.println("Entered in case");

          }
      }
      Text year_movies;
      IntWritable count_movies;
      for(int i=0;i<19;i++)
      {
        switch(i)
        {
          case 0:
              year_movies=new Text("2000\t"+key);
              //String app = year_movies.toString;
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 1:
              year_movies=new Text("2001\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 2:
              year_movies=new Text("2002\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 3:
              year_movies=new Text("2003\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 4:
              year_movies=new Text("2004\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 5:
              year_movies=new Text("2005\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 6:
              year_movies=new Text("2006\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 7:
              year_movies=new Text("2007\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 8:
              year_movies=new Text("2008\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 9:
              year_movies=new Text("2009\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 10:
              year_movies=new Text("2010\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 11:
              year_movies=new Text("2011\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 12:
              year_movies=new Text("2012\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 13:
              year_movies=new Text("2013\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 14:
              year_movies=new Text("2014\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 15:
              year_movies=new Text("2015\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 16:
              year_movies=new Text("2016\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 17:
              year_movies=new Text("2017\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;
          case 18:
              year_movies=new Text("2018\t"+key);
              //year_movies.append(key);
              count_movies=new IntWritable(count[i]);
              context.write(year_movies,count_movies);
              break;

        }
      }



//      int sum = 0;
//      for (IntWritable val : values) {
//        sum += val.get();
//      }
//      result.set(sum);
//      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
