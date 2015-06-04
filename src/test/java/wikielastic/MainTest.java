package wikielastic;

/**
 * Created by andrewcholakian on 5/31/15.
 */


import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class MainTest {
    @Test
    public void testTriple() {
        assertThat(Main.triple("AB"), equalTo("ABABAB"));
    }
}