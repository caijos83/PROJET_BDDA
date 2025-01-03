package miniSGBDR;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBConfig {
    private String dbpath;
    private int pagesize;
    private long dm_maxfilesize;
    private int bm_buffercount;
    public enum BMpolicy { LRU, MRU }
    private BMpolicy bm_policy;

    public DBConfig(String dbpath, int pagesize, long dm_maxfilesize, BMpolicy bm_policy, int bm_buffercount) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilesize = dm_maxfilesize;
        this.bm_policy = bm_policy;
        this.bm_buffercount = bm_buffercount;
    }

    public DBConfig(DBConfig other) {
        this.dbpath = other.dbpath;
        this.pagesize = other.pagesize;
        this.dm_maxfilesize = other.dm_maxfilesize;
        this.bm_policy = other.bm_policy;
        this.bm_buffercount = other.bm_buffercount;
    }

    public static DBConfig loadDBConfig(String filePath) throws IOException {
        String line;
        String dbpath = null;
        int pagesize = 0;
        long dm_maxfilesize = 0;
        BMpolicy bm_policy = null;
        int bm_buffercount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                if (parts.length < 2) continue;
                String key = parts[0].trim();
                String value = parts[1].trim();
                switch (key) {
                    case "dbpath":
                        dbpath = value;
                        break;
                    case "pagesize":
                        pagesize = Integer.parseInt(value);
                        break;
                    case "dm_maxfilesize":
                        dm_maxfilesize = Long.parseLong(value);
                        break;
                    case "bm_buffercount":
                        bm_buffercount = Integer.parseInt(value);
                        break;
                    case "bm_policy":
                        try {
                            bm_policy = BMpolicy.valueOf(value);
                        } catch (IllegalArgumentException e) {
                            System.err.println("Valeur de bm_policy invalide: " + value);
                        }
                        break;
                }
            }
        }

        if (bm_policy == null) {
            bm_policy = BMpolicy.LRU; 
        }

        return new DBConfig(dbpath, pagesize, dm_maxfilesize, bm_policy, bm_buffercount);
    }

    public String getDbpath() {
        return dbpath;
    }

    public int getPagesize() {
        return pagesize;
    }

    public long getDm_maxfilesize() {
        return dm_maxfilesize;
    }

    public BMpolicy getBm_policy() {
        return bm_policy;
    }

    public int getBm_buffercount() {
        return bm_buffercount;
    }

    public void printConfig() {
        System.out.println("DB Path: " + dbpath);
        System.out.println("Page Size: " + pagesize + " bytes");
        System.out.println("Max File Size: " + dm_maxfilesize + " bytes");
        System.out.println("BM Policy : " + bm_policy);
        System.out.println("Count Buffer: " + bm_buffercount);
    }

    public void setPagesize(int pz) {
        this.pagesize = pz;
    }
}
