package wikielastic.wiki;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by andrewcholakian on 6/1/15.
 */
@JsonSerialize
public class WikiPage {
    @JsonProperty("title")
    public volatile String title;
    @JsonProperty("text")
    public volatile StringBuilder text;
    @JsonProperty("redirect")
    public volatile String redirect;
    @JsonProperty("timestamp")
    public volatile String timestamp;
    @JsonProperty("ns")
    public volatile String ns;

    @JsonIgnore
    public boolean isRedirect() {
        return this.redirect != null;
    }

    @JsonIgnore
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
