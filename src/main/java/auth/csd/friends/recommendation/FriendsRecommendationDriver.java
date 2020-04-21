package auth.csd.friends.recommendation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FriendsRecommendationDriver {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(FriendsRecommendationDriver.class);
        job.setJobName("Friends Recommendation System in Facebook");
        job.setMapOutputKeyClass(Text.class );
        job.setMapOutputValueClass( Text.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        job.setMapperClass(FriendsRecommendationMapper.class);
        job.setReducerClass(FriendsRecommendationReducer.class);

        job.setInputFormatClass(TextInputFormat.class );
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath( job, new Path( args[0]));
        FileOutputFormat.setOutputPath( job, new Path( args[1]));

        System.exit( job.waitForCompletion(true) ? 0 : 1 );
    }
}
