package wikielastic.wiki;

/**
 * Created by andrewcholakian on 6/1/15.
 */
public class WikiRedirect implements WikiEntity {
    public final String title;
    public final String targetTitle;

    public WikiRedirect(String title, String targetTitle) {
        this.title = title;
        this.targetTitle = targetTitle;
    }

    @Override
    public WikiType getWikiType() {
        return WikiType.REDIRECT;
    }
}
