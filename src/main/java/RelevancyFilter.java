import java.io.Serializable;

public class RelevancyFilter implements Serializable {

    String[] keywords;

    public RelevancyFilter(String[] keywords) {
        this.keywords = keywords;
    }

    double getRelevance(String tweet) {
        double score = 0;
        String[] words = tweet.split(" ");
        for(String word: words) {
            for(String keyword: keywords) {
                if(word.equals(keyword)) {
                    score++;
                    break;
                }
            }
        }
        return score;
    }


}
