package wikielastic.wiki;

/**
 * Created by andrewcholakian on 6/1/15.
 */
public class WikiPage {
    public volatile String title;
    public volatile String text;
    public volatile String redirect;
    public volatile String timestamp;
    public volatile String ns;

    public boolean isRedirect() {
        return this.redirect != null;
    }

    public boolean isArticle() {
        return this.redirect == null;
    }

    @Override
    public String toString() {
        if (isRedirect()) {
            return String.format("Redirect<%s->%s>", this.title, this.redirect);
        } else {
            return String.format("Article<%s>", title);
        }
    }
}
