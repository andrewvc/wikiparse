package wikielastic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.temp.RedirectDb;
import wikielastic.wiki.WikiPage;
import wikielastic.wiki.WikiPageHandler;
import wikielastic.wiki.WikiParser;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class Main {
    public static Logger logger = LoggerFactory.getLogger(Main.class);
    public static WikiPage streamEnd = new WikiPage();

    public static void main(String[] args) {
        Processor.process("wikisample.xml");
    }
}