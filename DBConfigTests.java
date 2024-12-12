package tests;

import miniSGBDR.DBConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class DBConfigTests {

    public static void main(String[] args) throws IOException {
        testConstructor();
        testLoadDBConfig();
        testGetters();

        System.out.println("All DBConfigTests passed successfully!");
    }

    public static void testConstructor() {
        DBConfig config = new DBConfig(".\\Data", 4096, 1048576, DBConfig.BMpolicy.LRU, 5);

        assert config.getDbpath().equals(".\\Data") : "Dbpath mismatch!";
        assert config.getPagesize() == 4096 : "Pagesize mismatch!";
        assert config.getDm_maxfilesize() == 1048576 : "Maxfilesize mismatch!";
        assert config.getBm_policy() == DBConfig.BMpolicy.LRU : "BMpolicy mismatch!";
        assert config.getBm_buffercount() == 5 : "Buffercount mismatch!";

        System.out.println("Constructor test passed.");
    }

    public static void testLoadDBConfig() throws IOException {
        String testFilePath = "test_dbconfig.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(testFilePath))) {
            writer.println("dbpath = data/db");
            writer.println("pagesize = 4096");
            writer.println("dm_maxfilesize = 1048576");
            writer.println("bm_policy = LRU");
            writer.println("bm_buffercount = 10");
        }

        DBConfig config = DBConfig.loadDBConfig(testFilePath);

        assert config.getDbpath().equals("data/db") : "dbpath mismatch in file loading!";
        assert config.getPagesize() == 4096 : "pagesize mismatch in file loading!";
        assert config.getDm_maxfilesize() == 1048576 : "dm_maxfilesize mismatch in file loading!";
        assert config.getBm_policy() == DBConfig.BMpolicy.LRU : "bm_policy mismatch in file loading!";
        assert config.getBm_buffercount() == 10 : "bm_buffercount mismatch in file loading!";

        System.out.println("File loading test passed.");
    }

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
