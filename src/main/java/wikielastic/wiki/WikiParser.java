package wikielastic.wiki;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by andrewcholakian on 6/1/15.
 */
public class WikiParser implements Runnable {
    public static Logger logger = LoggerFactory.getLogger(WikiParser.class);

    public static void parse(WikiPageHandler handler, String filename) {
        XMLStreamReader xmlr = wikiReader(filename);
        WikiParser parser = new WikiParser(handler, xmlr);

        parser.run();
    }

    public static XMLStreamReader wikiReader(String filename) {
        InputStream fis;

        if (filename != null) {
            logger.info("Will read from XML dump file: " + filename);
            try {
                fis = new FileInputStream(filename);
                if (filename.endsWith(".gz")) {
                    try {
                        logger.info("Will decode file as gzip");
                        fis = new GZIPInputStream(fis);
                    } catch (IOException e) {
                        logger.warn("Error opening input stream as gzip");
                    }
                }
            } catch (FileNotFoundException e) {
                logger.error("Could not find file: " + filename);
                System.exit(1);
                return null;
            }
        } else {
            logger.info("Will read from stdin");
            fis = System.in;
        }


        XMLInputFactory xmlif = XMLInputFactory.newInstance();

        try {
            XMLStreamReader xmlr = xmlif.createXMLStreamReader(fis);

            return xmlr;
        } catch (XMLStreamException e) {
            logger.error("Hit an XML parsing exception", e);
            System.exit(1);
            return null;
        }
    }

    private WikiPageHandler handler;
    private XMLStreamReader xmlr;
    private Deque<String> ctx;
    public boolean started = false;
    private WikiPage curPage = null;

    WikiParser(WikiPageHandler handler, XMLStreamReader xmlr) {
        this.handler = handler;
        this.xmlr = xmlr;
        this.ctx = new ArrayDeque<String>();
    }

    @Override
    public void run() {
        // Only run once
        if (started) {
            return;
        }
        started = true;

        try {
            while (xmlr.hasNext()) {
                int eventType = xmlr.next();
                processEvent();
            }
            handler.handleEnd();
        } catch (XMLStreamException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void processEvent() {
        if (xmlr.isStartElement()) {
            String name = xmlr.getLocalName();
            ctx.push(name);

            if (name.equals("page")) {
                curPage = new WikiPage();
            }

            if (name.equals("redirect"))
                curPage.redirect = xmlr.getAttributeValue(null, "title");


        } else if (xmlr.isEndElement()) {
            ctx.pop();
            if (xmlr.getLocalName().equals("page")) {
                logger.debug("Finished parsing page: " + curPage);
                handler.handlePage(curPage);
            }
        } else if (xmlr.isCharacters() && !ctx.isEmpty()) {
            processCharacters(curPage, ctx.getFirst());
        }
    }

    private void processCharacters(WikiPage curPage, String currentCtx) {
        String text = xmlr.getText();

        if (!text.isEmpty()) {
            switch (currentCtx) {
                case "title":
                    curPage.title = curPage.title != null ? curPage.title + text : text;
                    break;
                case "text":
                    curPage.text = curPage.text != null ? curPage.text.append(text) : (new StringBuilder(text));
                    break;
                case "timestamp":
                    curPage.timestamp = curPage.timestamp != null ? curPage.timestamp + text : text;
                    break;
                case "ns":
                    curPage.ns = curPage.ns != null ? curPage.ns + text : text;
                    break;
            }
        }
    }
}
