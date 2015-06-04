package wikielastic.wiki;

/**
 * Created by andrewcholakian on 6/1/15.
 */
public interface WikiEntity {
    public enum WikiType {REDIRECT, PAGE};

    WikiType getWikiType();
}
