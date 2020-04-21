package auth.csd.friends.recommendation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class FriendsRecommendationMapper  extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOGGER = Logger.getLogger(FriendsRecommendationMapper.class.getName());

    @Override
    public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {

        String[] lineData = value.toString().split(" ");

        String userName = lineData[0];

        List<String> originalFriendsList = Arrays.asList(lineData[1].split(","));

        for (final String friend : originalFriendsList) {

            StringBuilder userNamePairs = new StringBuilder();
            userNamePairs.append(userName);
            userNamePairs.append("-");
            userNamePairs.append(friend);

            StringBuilder userNamePairsInverted = new StringBuilder();
            userNamePairsInverted.append(friend);
            userNamePairsInverted.append("-");
            userNamePairsInverted.append(userName);

            String friends = originalFriendsList.stream().filter(currentFriend -> !currentFriend.equals(friend))
                    .collect(Collectors.joining(","));

            StringBuilder friendsOfUserName = new StringBuilder();
            friendsOfUserName.append(userName);
            friendsOfUserName.append(":");
            friendsOfUserName.append(friends);

            LOGGER.info(String.format("The pair is %s - %s", userNamePairs, friendsOfUserName));
            LOGGER.info(String.format("The inverted pair is %s - %s", userNamePairsInverted, friendsOfUserName));

            context.write(new Text(userNamePairs.toString()), new Text(friendsOfUserName.toString()));
            context.write(new Text(userNamePairsInverted.toString()),new Text(friendsOfUserName.toString()));

        }

    }
}
