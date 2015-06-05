package wikielastic;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public interface StreamHandler<T> {
    public void handleItem(T item);
    public void handleEnd();
}
