import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CoffeeMapper
        extends Mapper<LongWritable, Text, Text, Text> {
    
    static AreaCode districts;
    
    static {
        try{
            Path pt_d = new Path("hdfs://babar.es.its.nyu.edu:8020/user/jy2234/CommunityAreas.txt");
            FileSystem fs_d = FileSystem.get(new Configuration());
            BufferedReader br_d = new BufferedReader(new InputStreamReader(fs_d.open(pt_d)));
            districts = new AreaCode(br_d);
        }catch(IOException e){
            e.printStackTrace();
        }

    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        String line = value.toString();
        String[] parts = line.split(",");
        String latitude = parts[0];
        String longitude = parts[1];

	String community = "x";
        if (!latitude.equals("NULL")) {
            community = districts.get(Double.parseDouble(latitude), Double.parseDouble(longitude));
        }

        String result = "";
        if (!community.equals("x")) {
            context.write(new Text(line), new Text(community));
        }
    }
}
