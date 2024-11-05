//package projet_SGBD;
import java.io.*;



public class DBConfig {

    private String dbpath;
    private int pagesize;
    private long dm_maxfilesize;
    private int bm_buffercount;
    public enum BMpolicy {
        LRU, MRU;
    }
    private BMpolicy bm_policy;


    public DBConfig(String dbpath, int pagesize, long dm_maxfilesize, BMpolicy bm_policy, int bm_buffercount) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilesize = dm_maxfilesize;
        this.bm_policy = bm_policy;
        this.bm_buffercount = bm_buffercount;
    }


    public static DBConfig loadDBConfig(String filePath) throws IOException {
        System.out.println(System.getProperty("user.dir"));
        String line;
        String dbpath = null;
        int pagesize = 0;
        long dm_maxfilesize = 0;
        BMpolicy bm_policy = null;
        int bm_buffercount = 0;
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        try {
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                switch (parts[0].trim()) {
                    case "dbpath":
                        dbpath = parts[1].trim();
                        // trim() est utilisé pour enlever les espaces blancs en début et fin de chaîne, assurant que les clés et valeurs sont correctement interprétées.
                        break;
                    case "pagesize":
                        pagesize = Integer.parseInt(parts[1].trim());
                        break;
                    case "dm_maxfilesize":
                        dm_maxfilesize = Long.parseLong(parts[1].trim());
                        break;
                    case "bm_buffercount":
                        bm_buffercount = Integer.parseInt(parts[1].trim());
                        break;
                    case "bm_policy":
                        try {
                            bm_policy = BMpolicy.valueOf(parts[1].trim());
                        } catch (IllegalArgumentException e) {
                            System.out.println("Valeur de bm_policy invalide: " + parts[1].trim());
                        }
                        break;

                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            // Gestion de l'exception : fichier introuvable
            System.out.println("Fichier introuvable: " + filePath);
        }
        catch (IOException e){
            // Gestion de l'exception
            e.printStackTrace();
        }finally {
            // fermeture du flux
            reader.close();
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



}