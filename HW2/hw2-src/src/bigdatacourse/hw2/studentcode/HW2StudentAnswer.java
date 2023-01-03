package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;

import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class HW2StudentAnswer implements HW2API {

    public class Item {

        private final JSONObject json;

        public Item(JSONObject json) {
            this.json = json;
        }

        public String getAsin() {
            try {
                return json.getString("asin");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public String getTitle() {
            try {
                return json.getString("title");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public String getImage() {
            try {
                return json.getString("imUrl");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public Set<String> getCategories() {
            try {
                JSONArray categoriesOuter = json.getJSONArray("categories");
                ArrayList<String> list = new ArrayList<>();

                for (int j = 0; j < categoriesOuter.length(); j++) {
                    JSONArray categoriesInner = categoriesOuter.getJSONArray(j);

                    for (int i = 0; i < categoriesInner.length(); i++) {
                        list.add(categoriesInner.getString(i));
                    }
                }

                return new TreeSet<>(list);
            } catch (JSONException e) {
                return new TreeSet<>();
            }
        }

        public String getDescription() {
            try {
                return json.getString("description");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

    }

    public class Review {

        private final JSONObject json;

        public Review(JSONObject json) {
            this.json = json;
        }

        public Instant getTime() {
            try {
                return Instant.ofEpochSecond(json.getLong("unixReviewTime"));
            } catch (JSONException e) {
                return Instant.ofEpochSecond(0); // TODO CHECK NOT AVAILABLE VALUE FOR INSTANT
            }
        }

        public String getAsin() {
            try {
                return json.getString("asin");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public String getReviewerID() {
            try {
                return json.getString("reviewerID");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public String getReviewerName() {
            try {
                return json.getString("reviewerName");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public int getRating() {
            try {
                return json.getInt("overall");
            } catch (JSONException e) {
                return -1;  // TODO CHECK NOT AVAILABLE VALUE FOR INT
            }
        }

        public String getSummary() {
            try {
                return json.getString("summary");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }

        public String getReviewText() {
            try {
                return json.getString("reviewText");
            } catch (JSONException e) {
                return NOT_AVAILABLE_VALUE;
            }
        }
    }

    // general consts

    private static final int MAX_THREADS = 250;
    public static final String NOT_AVAILABLE_VALUE = "na";
    private static final String TABLE_REVIEWS_BY_ASIN_NAME = "reviews_by_asin";
    private static final String TABLE_REVIEWS_BY_REVIEWER_ID_NAME = "reviews_by_reviewer_id";
    private static final String TABLE_ITEMS_NAME = "items";

    private static final String CQL_ITEMS_INSERT =
            "INSERT INTO " + TABLE_ITEMS_NAME + "(asin, title, image, categories, description) VALUES(?, ?, ?, ?, ?)";

    private static final String CQL_ITEMS_SELECT =
            "SELECT asin,title,image,categories,description FROM " + TABLE_ITEMS_NAME + " WHERE asin = ?";

    private static final String CQL_REVIEWS_BY_ASIN_INSERT =
            "INSERT INTO " + TABLE_REVIEWS_BY_ASIN_NAME + "(time, asin, reviewerID, reviewerName, rating, summary, reviewText) VALUES(?, ?, ?, ?, ?, ?, ?)";
    private static final String REVIEW_SELECT_COLUMNS = "time, asin, reviewerID,reviewerName, rating, summary, reviewText";

    private static final String CQL_REVIEWS_BY_ASIN_SELECT =
            "SELECT " + REVIEW_SELECT_COLUMNS + " FROM " + TABLE_REVIEWS_BY_ASIN_NAME + " WHERE asin = ?";

    private static final String CQL_REVIEWS_BY_REVIEWER_ID_INSERT =
            "INSERT INTO " + TABLE_REVIEWS_BY_REVIEWER_ID_NAME + "(time, asin, reviewerID, reviewerName, rating, summary, reviewText) VALUES(?, ?, ?, ?, ?, ?, ?)";

    private static final String CQL_REVIEWS_BY_REVIEWER_ID_SELECT =
            "SELECT " + REVIEW_SELECT_COLUMNS + " FROM " + TABLE_REVIEWS_BY_REVIEWER_ID_NAME + " WHERE reviewerID = ?";

    // CQL stuff

    private static final String CQL_CREATE_TABLE_ITEMS =
            "CREATE TABLE " + TABLE_ITEMS_NAME + "(" +
                    "asin text," +
                    "title text," +
                    "image text," +
                    "categories set<text>," +
                    "description text," +
                    "PRIMARY KEY (asin)" +
                    ") ";

    private static final String TABLE_REVIEWS_COLUMNS =
            "time timestamp," +
                    "asin text," +
                    "reviewerID text," +
                    "reviewerName text," +
                    "rating int," +
                    "summary text," +
                    "reviewText text,";
    private static final String CQL_CREATE_TABLE_REVIEWS_BY_ASIN =
            "CREATE TABLE " + TABLE_REVIEWS_BY_ASIN_NAME + "(" +
                    TABLE_REVIEWS_COLUMNS +
                    "PRIMARY KEY ((asin), time, reviewerID)" +
                    ") " +
                    "WITH CLUSTERING ORDER BY (time DESC, reviewerID ASC)";


    private static final String CQL_CREATE_TABLE_REVIEWS_BY_REVIEWER_ID =
            "CREATE TABLE " + TABLE_REVIEWS_BY_REVIEWER_ID_NAME + "(" +
                    TABLE_REVIEWS_COLUMNS +
                    "PRIMARY KEY ((reviewerID), time, asin)" +
                    ") " +
                    "WITH CLUSTERING ORDER BY (time DESC, asin ASC)";

    // cassandra session
    private CqlSession session;

    // prepared statements

    private PreparedStatement pstmtItemsInsert;
    private PreparedStatement pstmtReviewsByAsinInsert;
    private PreparedStatement pstmtReviewsByIdInsert;
    private PreparedStatement pstmtItemsSelect;
    private PreparedStatement pstmtReviewsByAsinSelect;
    private PreparedStatement pstmtReviewsByIdSelect;

    private static PreparedStatement[] reviewsPSTMT;
    private static String META_PRODUCTS_PATH = "\"HW2/data/meta_Office_Products.json\"";
    private static String REVIEWS_PRODUCTS_PATH = "\"HW2/data/reviews_Office_Products.json\"";

    @Override
    public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
        if (session != null) {
            System.out.println("ERROR - cassandra is already connected");
            return;
        }

        System.out.println("Initializing connection to Cassandra...");

        this.session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
                .withAuthCredentials(username, password)
                .withKeyspace(keyspace)
                .build();

        System.out.println("Initializing connection to Cassandra... Done");
    }


    @Override
    public void close() {
        if (session == null) {
            System.out.println("Cassandra connection is already closed");
            return;
        }

        System.out.println("Closing Cassandra connection...");
        session.close();
        System.out.println("Closing Cassandra connection... Done");
    }


    @Override
    public void createTables() {
        session.execute(CQL_CREATE_TABLE_ITEMS);
        session.execute(CQL_CREATE_TABLE_REVIEWS_BY_ASIN);
        session.execute(CQL_CREATE_TABLE_REVIEWS_BY_REVIEWER_ID);
        System.out.printf("created tables: %s, %s, %s\n", TABLE_ITEMS_NAME, TABLE_REVIEWS_BY_REVIEWER_ID_NAME, TABLE_REVIEWS_BY_ASIN_NAME);
    }

    @Override
    public void initialize() {
        pstmtItemsInsert = session.prepare(CQL_ITEMS_INSERT);
        pstmtReviewsByAsinInsert = session.prepare(CQL_REVIEWS_BY_ASIN_INSERT);
        pstmtReviewsByIdInsert = session.prepare(CQL_REVIEWS_BY_REVIEWER_ID_INSERT);
        pstmtItemsSelect = session.prepare(CQL_ITEMS_SELECT);
        pstmtReviewsByAsinSelect = session.prepare(CQL_REVIEWS_BY_ASIN_SELECT);
        pstmtReviewsByIdSelect = session.prepare(CQL_REVIEWS_BY_REVIEWER_ID_SELECT);
        reviewsPSTMT = new PreparedStatement[]{pstmtReviewsByAsinInsert, pstmtReviewsByIdInsert};
    }

    @Override
    public void loadItems(String pathItemsFile) throws Exception {
        // creating the thread factors
        ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

        ArrayList<JSONObject> json_items = parseData(pathItemsFile);

        for (JSONObject json_item : json_items) {

            executor.execute(new Runnable() {
                @Override
                public void run() {
//                    System.out.println("categories: " + new HashSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
                    Item item = new Item(json_item);

                    BoundStatement bstmt = pstmtItemsInsert.bind(
                            item.getAsin(),
                            item.getTitle(),
                            item.getImage(),
                            item.getCategories(),
                            item.getDescription());
//                            .setString(0, json.getString("asin")) // asin
//                            .setString(1, json.getString("title")) // title
//                            .setString(2, json.getString("imUrl")) // image
//                            .setSet(3, set, String) // categories
//                            .setString(4, json.getString("description")); // description

                    session.execute(bstmt);
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    public void loadReviews(String pathReviewsFile) throws Exception {
        // creating the thread factors
        ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

        ArrayList<JSONObject> reviews = parseData(pathReviewsFile);

        for (JSONObject json_review : reviews) {

            executor.execute(new Runnable() {
                @Override
                public void run() {
//                    System.out.println("categories: " + new HashSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
                    Review item = new Review(json_review);
                    for (PreparedStatement pstmt : reviewsPSTMT) {
                        BoundStatement bstmt = pstmt.bind(
                                item.getTime(),
                                item.getAsin(),
                                item.getReviewerID(),
                                item.getReviewerName(),
                                item.getRating(),
                                item.getSummary(),
                                item.getReviewText());
                        session.execute(bstmt);
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    public void item(String asin) {
        BoundStatement bstmt = pstmtItemsSelect.bind()
                .setString(0, asin);

        ResultSet rs = session.execute(bstmt);
        Row row = rs.one();

        if (row == null) {
            System.out.println("not exists");
        } else {
            System.out.println("asin: " + row.getString(0));
            System.out.println("title: " + row.getString(1));
            System.out.println("image: " + row.getString(2));
            System.out.println("categories: " + row.getSet(3, String.class));
            System.out.println("description: " + row.getString(4));
        }


//        // required format - example for asin B005QB09TU
//        System.out.println("asin: " 		+ "B005QB09TU");
//        System.out.println("title: " 		+ "Circa Action Method Notebook");
//        System.out.println("image: " 		+ "http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg");
//        System.out.println("categories: " 	+ new TreeSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
//        System.out.println("description: " 	+ "Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991.");;
//
//        // required format - if the asin does not exists return this value
//        System.out.println("not exists");
    }


    @Override
    public void userReviews(String reviewerID) {
        BoundStatement bstmt = pstmtReviewsByIdSelect.bind()
                .setString(0, reviewerID);

        ResultSet rs = session.execute(bstmt);
        Row row = rs.one();
        int count = 0;
        while (row != null) {
            printReview(row);
            row = rs.one();
            count++;
        }

        System.out.println("total reviews: " + count);

//        // required format - example for reviewerID A17OJCRPMYWXWV
//        System.out.println(
//                "time: " 			+ Instant.ofEpochSecond(1362614400) +
//                        ", asin: " 			+ "B005QDG2AI" 	+
//                        ", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
//                        ", reviewerName: " 	+ "Old Flour Child"	+
//                        ", rating: " 		+ 5 	+
//                        ", summary: " 		+ "excellent quality"	+
//                        ", reviewText: " 	+ "These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!");
//
//        System.out.println(
//                "time: " 			+ Instant.ofEpochSecond(1360108800) +
//                        ", asin: " 			+ "B003I89O6W" 	+
//                        ", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
//                        ", reviewerName: " 	+ "Old Flour Child"	+
//                        ", rating: " 		+ 5 	+
//                        ", summary: " 		+ "Checkbook Cover"	+
//                        ", reviewText: " 	+ "Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy.");
//
//        System.out.println("total reviews: " + 2);
    }

    @Override
    public void itemReviews(String asin) {
        BoundStatement bstmt = pstmtReviewsByAsinSelect.bind()
                .setString(0, asin);

        ResultSet rs = session.execute(bstmt);
        Row row = rs.one();
        int count = 0;
        while (row != null) {
            printReview(row);
            row = rs.one();
            count++;
        }

        System.out.println("total reviews: " + count);


//        // required format - example for asin B005QDQXGQ
//        System.out.println(
//                "time: " 			+ Instant.ofEpochSecond(1391299200) +
//                        ", asin: " 			+ "B005QDQXGQ" 	+
//                        ", reviewerID: " 	+ "A1I5J5RUJ5JB4B" 	+
//                        ", reviewerName: " 	+ "T. Taylor \"jediwife3\""	+
//                        ", rating: " 		+ 5 	+
//                        ", summary: " 		+ "Play and Learn"	+
//                        ", reviewText: " 	+ "The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it.");
//
//        System.out.println(
//                "time: " 			+ Instant.ofEpochSecond(1390694400) +
//                        ", asin: " 			+ "B005QDQXGQ" 	+
//                        ", reviewerID: " 	+ "AF2CSZ8IP8IPU" 	+
//                        ", reviewerName: " 	+ "Corey Valentine \"sue\""	+
//                        ", rating: " 		+ 1 	+
//                        ", summary: " 		+ "Not good"	+
//                        ", reviewText: " 	+ "This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy");
//
//        System.out.println(
//                "time: "			+ Instant.ofEpochSecond(1388275200) +
//                        ", asin: " 			+ "B005QDQXGQ" 	+
//                        ", reviewerID: " 	+ "A27W10NHSXI625" 	+
//                        ", reviewerName: " 	+ "Beth"	+
//                        ", rating: " 		+ 2 	+
//                        ", summary: " 		+ "Way overpriced for a beach ball"	+
//                        ", reviewText: " 	+ "It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway.");
//
//        System.out.println("total reviews: " + 3);
    }

    public void printReview(Row row) {
        System.out.println(
                "time: " + row.getInstant(0) +
                        ", asin: " + row.getString(1) +
                        ", reviewerID: " + row.getString(2) +
                        ", reviewerName: " + row.getString(3) +
                        ", rating: " + row.getInt(4) +
                        ", summary: " + row.getString(5) +
                        ", reviewText: " + row.getString(6));
    }

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
}
