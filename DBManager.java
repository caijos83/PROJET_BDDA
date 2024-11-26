import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class DBManager {
    private Map<String, Map<String, Relation>> databases; // Stocke les bases de données et leurs tables
    private String currentDatabase;
    private DBConfig config;

    // Constructeur
    public DBManager(DBConfig config) {
        this.config = config;
        this.databases = new HashMap<>();
        this.currentDatabase = null;
    }

    public void createTable(String tableName, List<String> columnNames, List<String> columnTypes, DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database. Use SET DATABASE first.");
            return;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);

        if (tables.containsKey(tableName)) {
            System.out.println("Table " + tableName + " already exists.");
            return;
        }

        Relation relation = new Relation(tableName);
        for (int i = 0; i < columnNames.size(); i++) {
            relation.addColumn(columnNames.get(i), columnTypes.get(i));
        }

        // Créer une nouvelle page pour le header de la table
        PageId headerPageId = diskManager.allocPage(tableName); // Alloue une page pour le header
        relation.setHeaderPageId(headerPageId);
        tables.put(tableName, relation);

    }

    public void dropTable(String tableName, DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);

        Relation relation = tables.remove(tableName);
        if (relation != null) {
            List<PageId> pageIds = diskManager.getPagesForTable(tableName); // Récupère les pages associées
            for (PageId pageId : pageIds) {
                diskManager.deallocPage(pageId); // Libère chaque page
            }
            System.out.println("Table " + tableName + " dropped from " + currentDatabase + ".");
        } else {
            System.out.println("Table " + tableName + " does not exist.");
        }
    }

    public void dropAllTables(DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database. Use SET DATABASE to select one.");
            return;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);

        // Parcourir et supprimer toutes les tables
        for (String tableName : new ArrayList<>(tables.keySet())) {
            dropTable(tableName, diskManager); // Réutilise dropTable
        }

        System.out.println("All tables in " + currentDatabase + " have been dropped.");
    }

    public void dropAllDatabases(DiskManager diskManager) throws IOException {
        for (String dbName : new ArrayList<>(databases.keySet())) {
            setCurrentDatabase(dbName);
            dropAllTables(diskManager);
            databases.remove(dbName);
        }
        currentDatabase = null;
        System.out.println("All databases have been dropped.");
    }


    // Créer une base de données
    public void createDatabase(String dbName) {
        if (!databases.containsKey(dbName)) {
            databases.put(dbName, new HashMap<>());
            System.out.println("Database " + dbName + " created.");
        }
    }

    // Supprimer une base de données
    public void removeDatabase(String dbName) {
        if (databases.containsKey(dbName)) {
            databases.remove(dbName);
            System.out.println("Database " + dbName + " removed.");

            if (dbName.equals(currentDatabase)) {
                currentDatabase = null;
                System.out.println("The active database has been reset.");
            }
        } else {
            System.out.println("Database " + dbName + " does not exist.");
        }
    }

    // Définir la base de données active
    public void setCurrentDatabase(String dbName) {
        if (databases.containsKey(dbName)) {
            currentDatabase = dbName;
            System.out.println("Current database set to " + dbName + ".");
        } else {
            System.out.println("Database " + dbName + " does not exist.");
        }
    }

    // Ajouter une table à la base de données active
    public void addTableToCurrentDatabase(Relation table) {
        if (currentDatabase == null) {
            System.out.println("No active database. Use SET DATABASE to select one.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        if (!tables.containsKey(table.getName())) {
            tables.put(table.getName(), table);
            System.out.println("Table " + table.getName() + " added to " + currentDatabase + ".");
        }
    }

    public String getCurrentDatabaseName() {
        return currentDatabase;
    }


    // Récupérer une table de la base de données active
    public Relation getTableFromCurrentDatabase(String tableName) {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return null;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        return tables.get(tableName);
    }


    // Supprimer une table de la base de données active
    public void removeTableFromCurrentDatabase(String tableName) {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        if (tables.remove(tableName) != null) {
            System.out.println("Table " + tableName + " removed from " + currentDatabase + ".");
        }
    }

    // Lister les bases de données
    public void listDatabases() {
        System.out.println("Databases:");
        for (String dbName : databases.keySet()) {
            System.out.println(dbName);
        }
    }

    // Lister les tables dans la base courante
    public void listTablesInCurrentDatabase() {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }
        System.out.println("Tables in " + currentDatabase + ":");
        Map<String, Relation> tables = databases.get(currentDatabase);
        for (Relation table : tables.values()) {
            System.out.println("- " + table.getName() + ": " + table.getColumnNames());
        }
    }

    // Sauvegarder l'état du DBManager
    public void saveState() throws IOException {
        String dbPath = config.getDbpath();
        File saveFile = new File(dbPath, "databases.save");

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(saveFile))) {
            oos.writeObject(databases); // Sauvegarde des bases de données
            oos.writeObject(currentDatabase); // Sauvegarde de la base active
        }
        System.out.println("State saved to " + saveFile.getAbsolutePath());
    }

    // Charger l'état du DBManager
    @SuppressWarnings("unchecked")
    public void loadState() throws IOException, ClassNotFoundException {
        String dbPath = config.getDbpath();
        File saveFile = new File(dbPath, "databases.save");

        if (!saveFile.exists()) {
            System.out.println("No saved state found. Starting fresh.");
            databases = new HashMap<>();
            currentDatabase = null;
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(saveFile))) {
            databases = (Map<String, Map<String, Relation>>) ois.readObject();
            currentDatabase = (String) ois.readObject();
        }
        System.out.println("State loaded from " + saveFile.getAbsolutePath());
    }

    /**
     * Sauvegarde une table (Relation) sur le disque en utilisant le DiskManager.
     * Les enregistrements de la table sont répartis sur des pages de taille fixe,
     * qui sont ensuite écrites séquentiellement sur le disque.
     *
     * @param relation   La table à sauvegarder, contenant ses métadonnées et ses enregistrements.
     * @param diskManager Instance de DiskManager pour gérer l'allocation et l'écriture des pages.
     *
     * Fonctionnement :
     * 1. Les enregistrements sont écrits dans un buffer de la taille d'une page.
     * 2. Lorsque le buffer est plein, il est écrit sur le disque, et un nouveau buffer est utilisé.
     * 3. À la fin, toute donnée restante dans le buffer est également écrite.
     */
    public void saveTableToDisk(Relation relation, DiskManager diskManager) throws IOException {
        int pageSize = config.getPagesize();
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        List<Record> records = relation.getRecords();
        PageId pageId = diskManager.allocPage(relation.getName()); // Utilise le nom de la table

        int pos = 0;
        for (Record record : records) {
            int bytesWritten = relation.writeRecordToBuffer(record, buffer, pos);
            pos += bytesWritten;

            if (pos >= pageSize) {
                buffer.flip();
                diskManager.writePage(pageId, buffer);
                buffer.clear();
                pos = 0;
                pageId = diskManager.allocPage(relation.getName()); // Utilise le nom de la table
            }
        }

        if (pos > 0) {
            buffer.flip();
            diskManager.writePage(pageId, buffer);
        }

        System.out.println("Table " + relation.getName() + " saved to disk.");
    }


    /**
     * Charge une table (Relation) depuis le disque en utilisant le DiskManager.
     * Cette méthode restaure les métadonnées et les enregistrements associés à la table,
     * puis l'ajoute à la base de données courante.
     *
     * @param tableName  Le nom de la table à charger depuis le disque.
     * @param diskManager Instance de DiskManager pour lire les pages associées à la table.
     * @return L'objet Relation restauré contenant les métadonnées et les enregistrements.
     * Fonctionnement :
     * 1. Vérifie que la base courante est active et que la table existe.
     * 2. Lit la page d'en-tête pour restaurer les métadonnées (colonnes et types).
     * 3. Charge les enregistrements depuis les pages associées à la table.
     * 4. Ajoute la table restaurée à la base de données courante.
     *
     * Exemple d'utilisation :
     * Relation relation = LoadTableFromDisk("Tab1", diskManager);
     */
    public Relation loadTableFromDisk(String tableName, DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return null;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);
        Relation relation = tables.get(tableName);

        if (relation == null) {
            System.out.println("Table " + tableName + " does not exist.");
            return null;
        }

        ByteBuffer buffer = ByteBuffer.allocate(config.getPagesize());
        PageId headerPageId = relation.getHeaderPageId();
        diskManager.readPage(headerPageId, buffer);

        buffer.flip();
        int columnCount = buffer.getInt();
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();

        for (int i = 0; i < columnCount; i++) {
            byte[] nameBytes = new byte[20];
            buffer.get(nameBytes);
            String[] colParts = new String(nameBytes).trim().split(":");
            columnNames.add(colParts[0]);
            columnTypes.add(colParts[1]);
        }

        relation = new Relation(tableName);
        for (int i = 0; i < columnNames.size(); i++) {
            relation.addColumn(columnNames.get(i), columnTypes.get(i));
        }

        relation.setHeaderPageId(headerPageId);
        tables.put(tableName, relation);

        System.out.println("Table " + tableName + " loaded from disk.");
        return relation;
    }
}
