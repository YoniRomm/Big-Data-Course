package bigdatacourse.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class JSONExample {

    public static ArrayList<JSONObject> parseData(String file_path) {
        ArrayList<JSONObject> items = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file_path))) {
            String line;
            while ((line = br.readLine()) != null)
                items.add(new JSONObject(line));

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return items;

    }

    public static void main(String[] args) {
        // you will find here a few examples to handle JSON.Org
        System.out.println("you will find here a few examples to handle JSON.org");

        // creating object examples
        JSONObject json = new JSONObject();                                // initialize empty object
        json = new JSONObject("{\"phone\":\"05212345678\"}");    // initialize from string

        System.out.println(System.currentTimeMillis());

        ArrayList<JSONObject> reviews = parseData("./HW2/data/meta_Office_Products.json");

        System.out.println(System.currentTimeMillis());

        // adding attributes
        json.put("street", "Einstein");
        json.put("number", 3);
        json.put("city", "Tel Aviv");
        System.out.println(json);                    // prints single line
        System.out.println(json.toString(4));        // prints "easy reading"

        // adding inner objects
        JSONObject main = new JSONObject();
        main.put("address", json);
        main.put("name", "Rubi Boim");
        System.out.println(main.toString(4));

        // adding array (1)
        JSONArray views = new JSONArray();
        views.put(1);
        views.put(2);
        views.put(3);
        main.put("views-simple", views);

        // adding array (2)
        JSONArray viewsExtend = new JSONArray();
        viewsExtend.put(new JSONObject().put("movieName", "American Pie").put("viewPercentage", 72));
        viewsExtend.put(new JSONObject().put("movieName", "Top Gun").put("viewPercentage", 100));
        viewsExtend.put(new JSONObject().put("movieName", "Bad Boys").put("viewPercentage", 87));
        main.put("views-extend", viewsExtend);

        System.out.println(main);
        System.out.println(main.toString(4));

    }


}
