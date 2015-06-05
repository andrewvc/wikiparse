package wikielastic.temp;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;
import wikielastic.wiki.WikiPage;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class ArticleDb {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    public final String filename;
    public final static Logger logger = LoggerFactory.getLogger(RedirectDb.class);
    OutputStream os;
    JsonGenerator jsonGenerator;

    public ArticleDb(String filename) {
        this.filename = filename;
    }

    public JsonGenerator initializeGenerator() throws IOException {
        os = new SnappyOutputStream(new FileOutputStream(filename));
        //os = new FileOutputStream(filename);

        SmileFactory f = new SmileFactory();

        jsonGenerator = f.createGenerator(os);

        jsonGenerator.writeStartArray();

        jsonGenerator.setCodec(objectMapper);
        return jsonGenerator;
    }

    public void writePage(WikiPage p) throws IOException {
        jsonGenerator.writeObject(p);

    }

    public void closeGenerator() throws IOException {
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
        os.close();
    }


}
