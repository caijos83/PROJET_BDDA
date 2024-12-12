import java.io.IOException;
import java.util.*;

public class SGBD {
    private DBManager dbManager;
    private DiskManager diskManager;
    private BufferManager bufferManager;

    public SGBD(DBConfig config) {
        this.dbManager = new DBManager(config);
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager); // Initialise bufferManager
    }

    // Méthode principale
    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("? "); // Prompt utilisateur
            String command = scanner.nextLine();

            if (command.equalsIgnoreCase("QUIT")) {
                System.out.println("Exiting SGBD...");
                quitSgbd();
                break;
            }

            processCommand(command);
        }
    }

    // Analyse et traitement des commandes
    private void processCommand(String command) {
        try {
            if (command.startsWith("CREATE DATABASE")) {
                String dbName = command.split(" ")[2];
                dbManager.createDatabase(dbName);
            } else if (command.startsWith("SET DATABASE")) {
                String dbName = command.split(" ")[2];
                dbManager.setCurrentDatabase(dbName);
            } else if (command.startsWith("CREATE TABLE")) {
                createTableCommand(command);
            } else if (command.startsWith("DROP TABLE")) {
                String tableName = command.split(" ")[2];
                dbManager.dropTable(tableName, diskManager);
            } else if (command.equals("LIST TABLES")) {
                dbManager.listTablesInCurrentDatabase();
            } else if (command.equals("DROP TABLES")) {
                dbManager.dropAllTables(diskManager);
            } else if (command.startsWith("INSERT INTO")) {
                insertIntoCommand(command);
            } else if (command.equals("DROP DATABASES")) {
                dbManager.dropAllDatabases(diskManager);
            } else if (command.equals("LIST DATABASES")) {
                dbManager.listDatabases();
            } else if (command.equals("FLUSH BUFFERS")) {
                bufferManager.flushBuffers();
                System.out.println("Buffers flushed successfully.");
            } else if (command.equals("SAVE STATE")) {
                dbManager.saveState();
                System.out.println("State saved successfully.");
            } else if (command.equals("LOAD STATE")) {
                dbManager.loadState();
                System.out.println("State loaded successfully.");
            } else {
                System.out.println("Unknown command: " + command);
            }
        } catch (Exception e) {
            System.out.println("Error processing command: " + e.getMessage());
        }
    }

    private void createTableCommand(String command) throws IOException {
        try {
            // Extraction des parties de la commande
            int startIndex = command.indexOf("(");
            int endIndex = command.length()-1;

            if (startIndex == -1 || endIndex == -1 || startIndex >= endIndex) {
                throw new IllegalArgumentException("Invalid CREATE TABLE command: missing or malformed parentheses.");
            }

            String tableName = command.substring(13, startIndex).trim();
            String columns = command.substring(startIndex + 1, endIndex).trim();

            if (tableName.isEmpty()) {
                throw new IllegalArgumentException("Table name cannot be empty.");
            }

            // Extraction des colonnes
            String[] columnDefs = columns.split(",");
            List<String> columnNames = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();

            for (String col : columnDefs) {
                String[] colParts = col.split(":");
                if (colParts.length != 2) {
                    throw new IllegalArgumentException("Invalid column definition: " + col);
                }

                String colName = colParts[0].trim();
                String colType = colParts[1].trim();

                // Validation des noms et types de colonnes
                if (colName.isEmpty() || colType.isEmpty()) {
                    throw new IllegalArgumentException("Column name or type cannot be empty: " + col);
                }

                // Validation du type de colonne
                if (!colType.matches("INT|REAL|CHAR\\(\\d+\\)|VARCHAR\\(\\d+\\)")) {
                    throw new IllegalArgumentException("Invalid column type: " + colType);
                }

                columnNames.add(colName);
                columnTypes.add(colType);
            }

            // Création de la table
            dbManager.createTable(tableName, columnNames, columnTypes, diskManager);
            System.out.println("Table " + tableName + " created in " + dbManager.getCurrentDatabaseName());
        } catch (Exception e) {
            throw new IOException("Error while creating table: " + e.getMessage(), e);
        }
    }


    private void insertIntoCommand(String command) throws IOException {
        try {
            // Extraction du nom de la table et des valeurs
            String[] parts = command.split(" ");
            String tableName = parts[2];
            int startIndex = command.indexOf("(");
            int endIndex = command.length()-1;


            if (startIndex == -1 || endIndex == -1 || startIndex >= endIndex) {
                throw new IllegalArgumentException("Invalid INSERT INTO command: missing or malformed parentheses.");
            }

            String[] values = command.substring(startIndex + 1, endIndex).split(",");
            List<Object> recordValues = new ArrayList<>();
            for (String value : values) {
                value = value.trim();
                if (value.matches("^\\d+$")) { // Entier
                    recordValues.add(Integer.parseInt(value));
                } else if (value.matches("^\\d+\\.\\d+$")) { // Réel
                    recordValues.add(Float.parseFloat(value));
                } else { // Chaîne
                    recordValues.add(value.replaceAll("^\"|\"$", ""));
                }
            }

            // Récupérer la table et valider les types
            Relation table = dbManager.getTableFromCurrentDatabase(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist.");
            }

            List<String> columnTypes = table.getColumnTypes();
            if (columnTypes.size() != recordValues.size()) {
                throw new IllegalArgumentException("Column count mismatch: expected " + columnTypes.size() + " but got " + recordValues.size());
            }

            // Validation des types
            for (int i = 0; i < columnTypes.size(); i++) {
                String type = columnTypes.get(i);
                Object value = recordValues.get(i);

                if (type.equals("INT") && !(value instanceof Integer)) {
                    throw new IllegalArgumentException("Column " + i + " expects INT but got " + value);
                } else if (type.equals("REAL") && !(value instanceof Float)) {
                    throw new IllegalArgumentException("Column " + i + " expects REAL but got " + value);
                } else if ((type.startsWith("CHAR") || type.startsWith("VARCHAR")) && !(value instanceof String)) {
                    throw new IllegalArgumentException("Column " + i + " expects CHAR/VARCHAR but got " + value);
                }
            }

            // Créer un Record et insérer dans la table
            Record record = new Record(recordValues);
            RecordId recordId = table.insertRecord(record);
            System.out.println("Record inserted at " + recordId);
        } catch (Exception e) {
            throw new IOException("Error while inserting record: " + e.getMessage(), e);
        }
    }



    // Quitter le SGBD proprement
    private void quitSgbd() {
        try {
            System.out.println("Saving state before exiting...");
            dbManager.saveState();       // Sauvegarde des bases de données
            bufferManager.flushBuffers(); // Vide les buffers (si applicable)
            System.out.println("State saved and buffers flushed. Exiting.");
        } catch (IOException e) {
            System.out.println("Error during quit: " + e.getMessage());
        }
    }

    // Méthode main
    public static void main(String[] args) {
        // Initialiser avec un exemple de configuration
        DBConfig config = new DBConfig("./BinData", 4096, 10_485_760, DBConfig.BMpolicy.LRU, 10);
        SGBD sgbd = new SGBD(config);
        sgbd.run();
    }
}
