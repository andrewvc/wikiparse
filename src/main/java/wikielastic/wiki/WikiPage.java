package wikielastic.wiki;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by andrewcholakian on 6/1/15.
 */
@JsonSerialize
public class WikiPage {
    @JsonProperty("title")
    public String title;
    @JsonProperty("text")
    public StringBuilder text;
    @JsonProperty("redirect")
    public String redirect;
    @JsonProperty("timestamp")
    public String timestamp;
    @JsonProperty("ns")
    public String ns;

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
