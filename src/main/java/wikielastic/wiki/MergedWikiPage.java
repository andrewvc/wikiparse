package wikielastic.wiki;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by andrewcholakian on 6/5/15.
 *
 *
 */
@JsonSerialize
public class MergedWikiPage extends WikiPage {
    @JsonProperty("redirects")
    public volatile List<String> redirects;
}
