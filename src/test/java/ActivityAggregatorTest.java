import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityAggregatorTest {

    @Test
    void testAggregateData() throws Exception {
        JSONArray testInputData = new JSONArray(new String(Files.readAllBytes(Paths.get("src/main/resources/inputTest.json"))));
        JSONArray expectedOutput = new JSONArray(new String(Files.readAllBytes(Paths.get("src/main/resources/outputTest.json"))));

        JSONArray actualOutput = Main.aggregateData(testInputData, "testOutput.json");

        assertEquals(expectedOutput.toString(), actualOutput.toString());
    }
}
