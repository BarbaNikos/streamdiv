import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by nick on 12/9/15.
 */
public class TweetFileProducer implements Serializable {

    Logger logger = LoggerFactory.getLogger(TweetFileProducer.class);

    private DateFormat format;

    private String pathToFile;

    private BufferedReader reader;

    private boolean finished;

    public TweetFileProducer(String pathToFile) {
        this.format = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy", Locale.ENGLISH);
        this.pathToFile = pathToFile;
        finished = false;
        reader = null;
    }

    public void init() {
        logger.info("initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            try {
                reader = new BufferedReader(new FileReader(input));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }else {
            logger.error("file not found");
        }
        finished = false;
    }

    public Values nextTuple() {
        if (reader == null)
            init();
        if (finished)
            return null;
        Values values = new Values();
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line != null) {
            if (line.indexOf('|') >= 0) {
                String[] attributes = line.split("\\|");
                Long timestamp = -1L;
                try {
                    timestamp = format.parse(attributes[0]).getTime();
                } catch (ParseException e) {
                    timestamp = -1L;
                    e.printStackTrace();
                }
//                if (attributes.length < 15 || timestamp == -1L) {
//                    System.out.println(attributes[attributes.length - 1]);
//                    return null;
//                }
                String tweet = attributes[1];
                values.add(timestamp);
                values.add(tweet);
                return values;
            }
        }else {
            finished = true;
            return null;
        }
        return null;
    }

}
