package auth.csd.friends.recommendation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class FriendsRecommendationReducer extends Reducer<Text, Text, Text, Text> {

    private static final Logger LOGGER = Logger.getLogger(FriendsRecommendationReducer.class.getName());

    @Override
    public void reduce(Text userNamesPair, Iterable<Text> friendLists, Context context) throws IOException, InterruptedException {

        String[] userNames = userNamesPair.toString().split("-");

        HashMap<String, Set<String>> userNamesFriendsHashMap = new HashMap<>();

        for (Text friendList : friendLists) {

            String[] friendsWithUserName = friendList.toString().split(":");
            String userName = friendsWithUserName[0];
            Set<String> friendsOfUserName = new HashSet<>(Arrays.asList(friendsWithUserName[1].split(",")));
            userNamesFriendsHashMap.put(userName, friendsOfUserName);
        }

        Set<String> friendsOfFirstUserNameSet = userNamesFriendsHashMap.get(userNames[0]);
        Set<String> friendsOfSecondUserNameSet = userNamesFriendsHashMap.get(userNames[1]);

        if (friendsOfFirstUserNameSet == null || friendsOfSecondUserNameSet == null) {
            return;
        }

        Set<String> recommendedFriendsSet = new HashSet<>(friendsOfFirstUserNameSet);
        recommendedFriendsSet.removeAll(friendsOfSecondUserNameSet);

        for (String recommendedFriend : recommendedFriendsSet) {
            LOGGER.info(String.format("The recommended friend for user %s is %s", userNames[0], recommendedFriend));
            context.write(new Text(userNames[0]), new Text(recommendedFriend));
        }
    }

}

