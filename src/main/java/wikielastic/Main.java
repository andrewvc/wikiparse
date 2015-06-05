package wikielastic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.wiki.WikiPage;

public class Main {
    public static Logger logger = LoggerFactory.getLogger(Main.class);
    public static WikiPage streamEnd = new WikiPage();

    public static void main(String[] args) {
        logger.info("Starting WikiParse");

        String fn;
        if (args.length > 0) {
            fn = args[0];
        } else {
            fn = null;
        }

        Processor.process(fn, false, true);

        logger.info("Finished processing");
    }
}