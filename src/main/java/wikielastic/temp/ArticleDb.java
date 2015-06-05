package wikielastic.temp;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import wikielastic.StreamHandler;
import wikielastic.wiki.MergedWikiPage;
import wikielastic.wiki.WikiPage;

import java.io.*;
import java.util.Iterator;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class ArticleDb {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    public final String filename;
    public final static Logger logger = LoggerFactory.getLogger(ArticleDb.class);
    private volatile OutputStream outputStream;
    private volatile InputStream inputStream;
    SmileGenerator smileGenerator;

    public ArticleDb(String filename) {
        this.filename = filename;
    }

    public SmileGenerator initializeGenerator() throws IOException {
        File file = new File(filename);
        if (file.exists()) file.delete();

        this.outputStream = new SnappyOutputStream(new FileOutputStream(filename));
        SmileFactory smileFactory = new SmileFactory();

        smileGenerator = smileFactory.createGenerator(outputStream);

        smileGenerator.writeStartArray();

        smileGenerator.setCodec(objectMapper);
        return smileGenerator;
    }

    public void writePage(WikiPage p) throws IOException {
        smileGenerator.writeObject(p);

    }

    public void closeGenerator() throws IOException {
        smileGenerator.writeEndArray();
        smileGenerator.close();
        outputStream.close();
    }

    public void parse(StreamHandler<MergedWikiPage> streamHandler) throws IOException {
        this.inputStream = new SnappyInputStream(new FileInputStream(filename));
        SmileFactory smileFactory = new SmileFactory();
        SmileParser parser = smileFactory.createParser(inputStream);
        parser.setCodec(objectMapper);

        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_ARRAY) {
            logger.error("Expected first token to be array start in article db!");
            System.exit(1);
        }
        parser.nextToken();

        Iterator<MergedWikiPage> objectIt = parser.readValuesAs(MergedWikiPage.class);
        while (objectIt.hasNext()) {
            MergedWikiPage next = objectIt.next();
            streamHandler.handleItem(next);
        }

        streamHandler.handleEnd();
        parser.close();
        inputStream.close();
    }


    public void closeParser() throws IOException {

    }
}
