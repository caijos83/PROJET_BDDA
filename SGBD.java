import java.io.IOException;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


public class SGBD {
    private DBManager dbManager;
    private DiskManager diskManager;
    private BufferManager bufferManager;

    // Constructeur
    public SGBD(DBConfig config) {
        this.dbManager = new DBManager(config);
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager); // Initialise bufferManager
    }

    // Méthode principale
    public void Run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("? "); // Prompt
            String command = scanner.nextLine();

            if (command.equalsIgnoreCase("QUIT")) {
                System.out.println("Exiting SGBD...");
                break;
            }

            ProcessCommand(command);
        }
    }

    // Analyse et traitement des commandes
    private void ProcessCommand(String command) {
        if (command.startsWith("CREATE DATABASE")) {
            String dbName = command.split(" ")[2];
            dbManager.CreateDatabase(dbName);
        } else if (command.startsWith("SET DATABASE")) {
            String dbName = command.split(" ")[2];
            dbManager.SetCurrentDatabase(dbName);
        } else if (command.startsWith("CREATE TABLE")) {
            try {
                String[] parts = command.split(" ");
                String tableName = parts[2];
                String columns = command.substring(command.indexOf("(") + 1, command.indexOf(")"));
                String[] columnDefs = columns.split(",");
                List<String> columnNames = new ArrayList<>();
                List<String> columnTypes = new ArrayList<>();

                for (String col : columnDefs) {
                    String[] colParts = col.split(":");
                    columnNames.add(colParts[0].trim());
                    columnTypes.add(colParts[1].trim());
                }

                dbManager.CreateTable(tableName, columnNames, columnTypes, diskManager);
            } catch (Exception e) {
                System.out.println("Error while creating table: " + e.getMessage());
            }
        } else if (command.startsWith("DROP TABLE")) {
            try {
                String tableName = command.split(" ")[2];
                dbManager.DropTable(tableName, diskManager);
            } catch (Exception e) {
                System.out.println("Error while dropping table: " + e.getMessage());
            }
        } else if (command.equals("LIST TABLES")) {
            dbManager.ListTablesInCurrentDatabase();
        } else if (command.equals("DROP TABLES")) {
            try {
                dbManager.DropAllTables(diskManager);
            } catch (IOException e) {
                System.out.println("Error while dropping all tables: " + e.getMessage());
            }
        } else if (command.equals("DROP DATABASES")) {
            try {
                dbManager.DropAllDatabases(diskManager);
            } catch (Exception e) {
                System.out.println("Error while dropping databases: " + e.getMessage());
            }
        }
        else if (command.equals("LIST DATABASES")) {
            dbManager.ListDatabases();
        } else if (command.equals("FLUSH BUFFERS")) {
            try {
                bufferManager.FlushBuffers();
                System.out.println("Buffers flushed successfully.");
            } catch (IOException e) {
                System.out.println("Error while flushing buffers: " + e.getMessage());
            }
        }

        else if (command.equals("SAVE STATE")) {
            try {
                dbManager.SaveState();
                System.out.println("State saved successfully.");
            } catch (IOException e) {
                System.out.println("Error saving state: " + e.getMessage());
            }
        } else if (command.equals("LOAD STATE")) {
            try {
                dbManager.LoadState();
                System.out.println("State loaded successfully.");
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Error loading state: " + e.getMessage());
            }
        } else if (command.equals("QUIT")) {
            try {
                System.out.println("Saving state before exiting...");
                dbManager.SaveState();       // Sauvegarde des bases de données
                bufferManager.FlushBuffers(); // Vide les buffers (si applicable)
                System.out.println("Exiting SGBD.");
            } catch (IOException e) {
                System.out.println("Error during quit: " + e.getMessage());
            }
            System.exit(0);
        } else {
            System.out.println("Unknown command: " + command);
        }
    }


    // Méthode main
    public static void main(String[] args) {
        DBConfig config = new DBConfig("./DB", 4096, 10_485_760, 10, "LRU");
        SGBD sgbd = new SGBD(config);
        sgbd.Run();
    }
}
