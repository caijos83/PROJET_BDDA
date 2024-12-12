package miniSGBDR;

import java.io.*;
import java.util.*;

public class DBManager {
    private Map<String, Map<String, Relation>> databases; 
    private String currentDatabase;
    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;

    public DBManager(DBConfig config, DiskManager diskManager, BufferManager bufferManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.databases = new HashMap<>();
        this.currentDatabase = null;
    }

    public void CreateDatabase(String nomBdd) {
        if (!databases.containsKey(nomBdd)) {
            databases.put(nomBdd, new HashMap<>());
        }
    }

    public void SetCurrentDatabase(String nomBdd) {
        if (databases.containsKey(nomBdd)) {
            currentDatabase = nomBdd;
        } else {
            System.out.println("Database " + nomBdd + " does not exist.");
        }
    }

    public void AddTableToCurrentDatabase(Relation tab) {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        tables.put(tab.getName(), tab);
    }

    public Relation GetTableFromCurrentDatabase(String nomTable) {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return null;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        return tables.get(nomTable);
    }

    public void RemoveTableFromCurrentDatabase(String nomTable) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        Relation rel = tables.remove(nomTable);
        if (rel != null) {
            for (PageId pid : rel.getDataPages()) {
                diskManager.deallocPage(pid);
            }
            PageId headerId = rel.getHeaderPageId();
            if (headerId != null) {
                diskManager.deallocPage(headerId);
            }
        }
    }

    public void RemoveDatabase(String nomBdd) throws IOException {
        Map<String, Relation> tables = databases.remove(nomBdd);
        if (tables != null) {
            for (String tableName : tables.keySet()) {
                Relation rel = tables.get(tableName);
                for (PageId pid : rel.getDataPages()) {
                    diskManager.deallocPage(pid);
                }
                PageId headerId = rel.getHeaderPageId();
                if (headerId != null) {
                    diskManager.deallocPage(headerId);
                }
            }
            if (nomBdd.equals(currentDatabase)) {
                currentDatabase = null;
            }
        } else {
            System.out.println("Database " + nomBdd + " does not exist.");
        }
    }

    public void RemoveTablesFromCurrentDatabase() throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);
        List<String> tNames = new ArrayList<>(tables.keySet());
        for (String t : tNames) {
            RemoveTableFromCurrentDatabase(t);
        }
    }

    public void RemoveDatabases() throws IOException {
        List<String> dbNames = new ArrayList<>(databases.keySet());
        for (String db : dbNames) {
            SetCurrentDatabase(db);
            RemoveTablesFromCurrentDatabase();
            databases.remove(db);
        }
        currentDatabase = null;
    }

    public void ListDatabases() {
        for (String dbName : databases.keySet()) {
            System.out.println(dbName);
        }
    }

    public void ListTablesInCurrentDatabase() {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        for (Relation r : tables.values()) {
            StringBuilder sb = new StringBuilder();
            sb.append(r.getName()).append(" (");
            for (int i = 0; i < r.getColumnCount(); i++) {
                sb.append(r.getColumnNames().get(i)).append(":").append(r.getColumnTypes().get(i));
                if (i < r.getColumnCount() - 1) sb.append(",");
            }
            sb.append(")");
            System.out.println(sb.toString());
        }
    }

    public void SaveState() throws IOException {
        String dbPath = config.getDbpath();
        File saveFile = new File(dbPath, "databases.save");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(saveFile))) {
            oos.writeObject(databases);
            oos.writeObject(currentDatabase);
        }
    }

    @SuppressWarnings("unchecked")
    public void LoadState() throws IOException, ClassNotFoundException {
        String dbPath = config.getDbpath();
        File saveFile = new File(dbPath, "databases.save");
        if (!saveFile.exists()) {
            databases = new HashMap<>();
            currentDatabase = null;
            return;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(saveFile))) {
            databases = (Map<String, Map<String, Relation>>) ois.readObject();
            currentDatabase = (String) ois.readObject();
        }
    }

    public String getCurrentDatabaseName() {
        return currentDatabase;
    }

    public void CreateTable(String tableName, List<String> colNames, List<String> colTypes) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database. Use SET DATABASE first.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        if (tables.containsKey(tableName)) {
            System.out.println("Table " + tableName + " already exists.");
            return;
        }
        PageId headerPageId = diskManager.allocPage();
        Relation relation = new Relation(tableName, colNames, colTypes, headerPageId, diskManager, bufferManager, config);
        relation.initHeaderPage();

        tables.put(tableName, relation);
    }

    public void DropTable(String tableName) throws IOException {
        RemoveTableFromCurrentDatabase(tableName);
    }

    public DBConfig getConfig() {
        return config;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

}
