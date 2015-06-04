package wikielastic.wiki;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by andrewcholakian on 6/1/15.
 */
public class WikiParser implements Runnable {
    public static Logger logger = LoggerFactory.getLogger(WikiParser.class);

    public static void parse(WikiPageHandler handler, String filename) {
        XMLStreamReader xmlr = wikiReader("wikisample.xml");
        WikiParser parser = new WikiParser(handler, xmlr);

        parser.run();
    }

    public static XMLStreamReader wikiReader(String filename) {
        FileInputStream fis = null;

        try {
            fis = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            logger.error("Could not find file: " + filename);
            System.exit(1);
            return null;
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
            String currentCtx = ctx.getFirst();
            String text = xmlr.getText();

            if (!text.isEmpty()) {
                switch (currentCtx) {
                    case "title":
                        curPage.title = text;
                        break;
                    case "text":
                        curPage.text = text;
                        break;
                    case "timestamp":
                        curPage.timestamp = text;
                        break;
                    case "ns":
                        curPage.ns = text;
                        break;
                }
            }
        }
    }
}
