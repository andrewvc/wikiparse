package wikielastic.wiki;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public interface WikiPageHandler {
    public void handlePage(WikiPage page);
    public void handleEnd();
}
