import org.junit.Test;

import static org.junit.Assert.*;

public class NationalityCheckerTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        input[0] = "file:///D:/DS503/FaceInPage.csv";
        input[1] = "file:///D:/DS503/Project1/output";
        input[2] = "Viet Nam";

        NationalityChecker nc = new NationalityChecker();
        nc.debug(input);
    }
}