import java.nio.ByteBuffer;
import java.util.*;
import java.io.*;



public class DBManager {
    private Map<String, Map<String, Relation>> databases; // Stocke les bases de données et leurs tables
    private String currentDatabase;
    private DBConfig config;

    // Constructeur
    public DBManager(DBConfig config) {
        this.config = config;
        databases = new HashMap<>();
        currentDatabase = null;
    }

//    public DBManager() {
//        this.databases = new HashMap<>(); // Initialisation de databases
//        this.currentDatabase = null;     // Aucun DB actif par défaut
//    }



    public void CreateDatabase(String nomBdd) {
        if (!databases.containsKey(nomBdd)) {
            databases.put(nomBdd, new HashMap<>());
            System.out.println("Database " + nomBdd + " created.");
        }
    }


    public void RemoveDatabase(String dbName) {
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


    public void SetCurrentDatabase(String nomBdd) {
        if (databases.containsKey(nomBdd)) {
            currentDatabase = nomBdd;
            System.out.println("Current database set to " + nomBdd + ".");
        } else {
            System.out.println("Database " + nomBdd + " does not exist.");
        }
    }


    public void AddTableToCurrentDatabase(Relation table) {
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


    public Relation GetTableFromCurrentDatabase(String nomTable) {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return null;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        return tables.get(nomTable);
    }


    public void RemoveTableFromCurrentDatabase(String nomTable) {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }
        Map<String, Relation> tables = databases.get(currentDatabase);
        if (tables.remove(nomTable) != null) {
            System.out.println("Table " + nomTable + " removed from " + currentDatabase + ".");
        }
    }

    // Lister les bases de données
    public void ListDatabases() {
        System.out.println("Databases:");
        for (String dbName : databases.keySet()) {
            System.out.println(dbName);
        }
    }

    // Lister les tables dans la base courante
    public void ListTablesInCurrentDatabase() {
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


    public void SaveState() throws IOException {
        String dbPath = config.getDbpath(); // Récupère le chemin de dbpath à partir de DBConfig
        File saveFile = new File(dbPath, "databases.save"); // Fichier unique databases.save

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(saveFile))) {
            // Sauvegarde des bases de données
            for (String dbName : databases.keySet()) {
                oos.writeObject(dbName); // Écrit le nom de la base
                Map<String, Relation> tables = databases.get(dbName);
                oos.writeObject(tables); // Écrit les tables (Relation sérialisable)
            }
            // Sauvegarde de la base active
            oos.writeObject(currentDatabase);
        }
        System.out.println("State saved to " + saveFile.getAbsolutePath());
    }



    @SuppressWarnings("unchecked")
    public void LoadState() throws IOException, ClassNotFoundException {
        String dbPath = config.getDbpath(); // Récupère le chemin de dbpath
        File saveFile = new File(dbPath, "databases.save");

        if (!saveFile.exists()) {
            System.out.println("No saved state found. Starting fresh.");
            databases = new HashMap<>();
            currentDatabase = null;
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(saveFile))) {
            databases = new HashMap<>();
            while (true) {
                try {
                    String dbName = (String) ois.readObject(); // Récupère le nom de la base
                    Map<String, Relation> tables = (Map<String, Relation>) ois.readObject(); // Récupère les tables
                    databases.put(dbName, tables);
                } catch (EOFException e) {
                    break; // Fin du fichier
                }
            }
            currentDatabase = (String) ois.readObject(); // Récupère la base active
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
    public void SaveTableToDisk(Relation relation, DiskManager diskManager) throws IOException {
        int pageSize = config.getPagesize(); // Taille des pages, issue de DBConfig
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        List<Record> records = relation.getRecords(); // Implémenter une méthode dans Relation pour récupérer les records
        PageId pageId = diskManager.AllocPage("TableName"); // Allouer une nouvelle page pour commencer

        int pos = 0;
        for (Record record : records) {
            // Écrire l'enregistrement dans le buffer
            int bytesWritten = relation.writeRecordToBuffer(record, buffer, pos);
            pos += bytesWritten;

            // Si la page est pleine, écris-la sur le disque
            if (pos >= pageSize) {
                buffer.flip(); // Prépare le buffer pour l'écriture
                diskManager.WritePage(pageId, buffer);
                buffer.clear(); // Réinitialise le buffer
                pos = 0;        // Redémarre la position
                pageId = diskManager.AllocPage("TableName"); // Alloue une nouvelle page
            }
        }

        // Écris la dernière page si elle contient des données
        if (pos > 0) {
            buffer.flip();
            diskManager.WritePage(pageId, buffer);
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
    public Relation LoadTableFromDisk(String tableName, DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return null;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);

        // Vérifie si la table existe dans la base courante
        if (!tables.containsKey(tableName)) {
            System.out.println("Table " + tableName + " does not exist on disk.");
            return null;
        }

        Relation relation = tables.get(tableName);
        ByteBuffer buffer = ByteBuffer.allocate(config.getPagesize());

        // Lire la page d'en-tête pour récupérer les métadonnées
        PageId headerPageId = relation.getHeaderPageId();
        diskManager.ReadPage(headerPageId, buffer);

        // Restaurer les métadonnées (exemple simplifié)
        buffer.flip();
        int columnCount = buffer.getInt();
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();

        for (int i = 0; i < columnCount; i++) {
            byte[] nameBytes = new byte[20]; // Exemple : limite 20 caractères pour les noms de colonnes
            buffer.get(nameBytes);
            String columnInfo = new String(nameBytes).trim();
            String[] colParts = columnInfo.split(":");
            columnNames.add(colParts[0]);
            columnTypes.add(colParts[1]);
        }

        // Met à jour la relation avec les métadonnées restaurées
        relation = new Relation(tableName);
        for (int i = 0; i < columnNames.size(); i++) {
            relation.addColumn(columnNames.get(i), columnTypes.get(i));
        }
        relation.setHeaderPageId(headerPageId);

        // Lire les données des pages associées à la table
        List<PageId> pageIds = diskManager.GetPagesForTable(tableName); // Méthode fictive, à implémenter dans DiskManager
        for (PageId pageId : pageIds) {
            buffer.clear();
            diskManager.ReadPage(pageId, buffer);
            buffer.flip();

            int pos = 0;
            while (pos < buffer.limit()) {
                Record record = new Record();
                int bytesRead = relation.readFromBuffer(record, buffer, pos);
                pos += bytesRead;

                // Ajouter l'enregistrement à la table
                relation.addRecord(record); // Implémenter une méthode pour ajouter des records
            }
        }

        // Ajoute la table restaurée à la base de données courante
        tables.put(tableName, relation);
        System.out.println("Table " + tableName + " loaded from disk.");
        return relation;
    }


    public void CreateTable(String tableName, List<String> columnNames, List<String> columnTypes, DiskManager diskManager) throws IOException {
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

        PageId headerPageId = diskManager.AllocPage(tableName);
        relation.setHeaderPageId(headerPageId);
        tables.put(tableName, relation);

        System.out.println("Table " + tableName + " created in " + currentDatabase + ".");
    }


    public void DropTable(String tableName, DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database.");
            return;
        }

        Map<String, Relation> tables = databases.get(currentDatabase);

        Relation relation = tables.remove(tableName);
        if (relation != null) {
            List<PageId> pageIds = diskManager.GetPagesForTable(tableName); // Méthode à implémenter dans DiskManager
            for (PageId pageId : pageIds) {
                diskManager.FreePage(pageId);
            }
            System.out.println("Table " + tableName + " dropped from " + currentDatabase + ".");
        } else {
            System.out.println("Table " + tableName + " does not exist.");
        }
    }


    public void DropAllTables(DiskManager diskManager) throws IOException {
        if (currentDatabase == null) {
            System.out.println("No active database. Use SET DATABASE to select one.");
            return;
        }

        // Récupère les tables de la base de données courante
        Map<String, Relation> tables = databases.get(currentDatabase);

        // Parcours et suppression de chaque table
        for (String tableName : new ArrayList<>(tables.keySet())) {
            DropTable(tableName, diskManager); // Réutilise DropTable
        }

        System.out.println("All tables in " + currentDatabase + " have been dropped.");
    }



    public void DropAllDatabases(DiskManager diskManager) throws IOException {
        for (String dbName : new ArrayList<>(databases.keySet())) {
            SetCurrentDatabase(dbName);
            DropAllTables(diskManager);
            databases.remove(dbName);
        }
        currentDatabase = null;
        System.out.println("All databases have been dropped.");
    }

}
