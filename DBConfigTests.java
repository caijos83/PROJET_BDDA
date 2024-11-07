package miniSGBDR;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class DBConfigTests {

    public static void main(String[] args) throws IOException {
        testConstructor();
        testLoadDBConfig();
        testGetters();
    }

    // Test the constructor
    public static void testConstructor() {
        DBConfig config = new DBConfig("C:\\Users\\manon\\OneDrive\\Documents\\L3\\Data", 4096, 1048576, DBConfig.BMpolicy.LRU, 5);

        assert config.getDbpath().equals("data") : "Dbpath mismatch!";
        assert config.getPagesize() == 4096 : "Pagesize mismatch!";
        assert config.getDm_maxfilesize() == 1048576 : "Maxfilesize mismatch!";
        assert config.getBm_policy() == DBConfig.BMpolicy.LRU : "BMpolicy mismatch!";
        assert config.getBm_buffercount() == 5 : "Buffercount mismatch!";

        System.out.println("Constructor test passed.");
    }

    // Test loading configuration from a file
    public static void testLoadDBConfig() throws IOException {
        String testFilePath = "test_dbconfig.txt";

        // Create a sample configuration file
        try (PrintWriter writer = new PrintWriter(new FileWriter(testFilePath))) {
            writer.println("dbpath = data/db");
            writer.println("pagesize = 4096");
            writer.println("dm_maxfilesize = 1048576");
            writer.println("bm_policy = LRU");
            writer.println("bm_buffercount = 10");
        }

        // Load configuration from the file
        DBConfig config = DBConfig.loadDBConfig(testFilePath);

        // Assertions to check loaded values
        assert config.getDbpath().equals("data/db") : "dbpath mismatch in file loading!";
        assert config.getPagesize() == 4096 : "pagesize mismatch in file loading!";
        assert config.getDm_maxfilesize() == 1048576 : "dm_maxfilesize mismatch in file loading!";
        assert config.getBm_policy() == DBConfig.BMpolicy.LRU : "bm_policy mismatch in file loading!";
        assert config.getBm_buffercount() == 10 : "bm_buffercount mismatch in file loading!";

        System.out.println("File loading test passed.");
    }

    // Test getters
    public static void testGetters() {
        DBConfig config = new DBConfig("data/db", 2048, 2097152, DBConfig.BMpolicy.MRU, 8);

        assert config.getDbpath().equals("data/db") : "getDbpath failed!";
        assert config.getPagesize() == 2048 : "getPagesize failed!";
        assert config.getDm_maxfilesize() == 2097152 : "getDm_maxfilesize failed!";
        assert config.getBm_policy() == DBConfig.BMpolicy.MRU : "getBm_policy failed!";
        assert config.getBm_buffercount() == 8 : "getBm_buffercount failed!";

        System.out.println("Getters test passed.");
    }
}
