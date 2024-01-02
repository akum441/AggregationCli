import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class Main {

    public static void main(String[] args) throws Exception {
        String inputFilePath = "/input.json";
        String outputFilePath = "output.json";

        JSONArray inputData = readJsonFile(inputFilePath);
        JSONArray outputData = aggregateData(inputData, outputFilePath);

        Files.write(Paths.get(outputFilePath), outputData.toString().getBytes());
    }

    private static JSONArray readJsonFile(String filePath) throws Exception {
        InputStream inputStream = Main.class.getResourceAsStream(filePath);
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        String jsonText = new Scanner(inputStream).useDelimiter("\\A").next();
        return new JSONArray(jsonText);
    }

    static JSONArray aggregateData(JSONArray inputData, String outputFilePath) throws Exception {
        ConcurrentHashMap<String, Map<String, Integer>> userDateEvents = new ConcurrentHashMap<>();

        // Load existing data from output.json
        if (Files.exists(Paths.get(outputFilePath))) {
            String existingData = readFileAsString(Paths.get(outputFilePath));
            JSONArray existingArray = new JSONArray(existingData);
            IntStream.range(0, existingArray.length()).parallel().forEach(i -> {
                JSONObject item = null;
                try {
                    item = existingArray.getJSONObject(i);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                try {
                    processExistingItem(item, userDateEvents);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

        }

        // Process new input data
        IntStream.range(0, inputData.length()).parallel().forEach(i -> {
            JSONObject jsonItem = null;
            try {
                jsonItem = inputData.getJSONObject(i);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                processInputItem(jsonItem, userDateEvents);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        });

        return buildOutputJson(userDateEvents);
    }

    private static String readFileAsString(Path path) throws IOException, IOException {
        StringBuilder contentBuilder = new StringBuilder();

        try (BufferedReader br = Files.newBufferedReader(path)) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                contentBuilder.append(currentLine).append("\n");
            }
        }

        return contentBuilder.toString();
    }


    private static void processExistingItem(JSONObject item, Map<String, Map<String, Integer>> userDateEvents) throws JSONException {
        String userDateKey = item.getInt("userId") + "_" + item.getString("date");
        Map<String, Integer> eventMap = new HashMap<>();
        Iterator<String> keys = item.keys();

        while (keys.hasNext()) {
            String eventType = keys.next();
            if (!eventType.equals("userId") && !eventType.equals("date")) {
                eventMap.put(eventType, item.getInt(eventType));
            }
        }

        userDateEvents.put(userDateKey, eventMap);

    }

    private static void processInputItem(JSONObject jsonItem, Map<String, Map<String, Integer>> userDateEvents) throws JSONException {
        int userId = jsonItem.getInt("userId");
        String eventType = jsonItem.getString("eventType");
        LocalDate date = Instant.ofEpochSecond(jsonItem.getLong("timestamp"))
                .atZone(ZoneId.systemDefault()).toLocalDate();

        String key = userId + "_" + date;
        userDateEvents.computeIfAbsent(key, k -> new HashMap<>())
                .merge(eventType, 1, Integer::sum);
    }
    private static JSONArray buildOutputJson(Map<String, Map<String, Integer>> userDateEvents) {
        // Custom comparator for sorting by userId and then date
        Comparator<String> comparator = (key1, key2) -> {
            String[] parts1 = key1.split("_");
            String[] parts2 = key2.split("_");
            int userIdCompare = parts1[0].compareTo(parts2[0]);
            if (userIdCompare != 0) {
                return userIdCompare;
            }
            return parts1[1].compareTo(parts2[1]);
        };

        // Using TreeMap to sort based on the comparator
        Map<String, Map<String, Integer>> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(userDateEvents);

        JSONArray outputArray = new JSONArray();
        sortedMap.forEach((key, events) -> {
            String[] parts = key.split("_");
            JSONObject jsonItem = new JSONObject();
            try {
                jsonItem.put("userId", Integer.parseInt(parts[0]));
                jsonItem.put("date", parts[1]);
            } catch (JSONException e) {
                e.printStackTrace();
            }

            events.forEach((_key, value) -> {
                try {
                    jsonItem.put(_key, value);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

            outputArray.put(jsonItem);
        });

        return outputArray;
    }

}
