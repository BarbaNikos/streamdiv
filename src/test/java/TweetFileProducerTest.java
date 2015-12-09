import backtype.storm.tuple.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Created by nick on 12/9/15.
 */
public class TweetFileProducerTest {

    TweetFileProducer producer;

    @Before
    public void setUp() throws Exception {
        producer = new TweetFileProducer("data" + File.separator + "tweet_file_0.txt");
        producer.init();
    }

    @Test
    public void testNextTuple() throws Exception {
        for (int i = 0; i < 100; i++) {
            Values values = producer.nextTuple();
            if (values != null)
                System.out.println(values.toString());
        }
    }
}
