package tests;

import miniSGBDR.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DBManagerTests {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // Initialisation
        DBConfig config = new DBConfig(".\\Data", 4096, 20480, DBConfig.BMpolicy.LRU, 5);
        DiskManager diskManager = new DiskManager(config);
        BufferManager bufferManager = new BufferManager(config, diskManager);

        DBManager dbManager = new DBManager(config, diskManager, bufferManager);

        testCreateAndSelectDatabase(dbManager);
        testCreateTable(dbManager);
        testListDatabasesTables(dbManager);
        testDropTable(dbManager);
        testDropDatabase(dbManager);
        testRemoveAllDatabases(dbManager);
        testSaveLoadState(dbManager);

        System.out.println("All DBManagerTests passed successfully!");
    }

    public static void testCreateAndSelectDatabase(DBManager dbManager) {
        dbManager.CreateDatabase("TestDB");
        dbManager.SetCurrentDatabase("TestDB");
        assert dbManager.getCurrentDatabaseName().equals("TestDB") : "Current database should be TestDB!";
        System.out.println("testCreateAndSelectDatabase passed.");
    }

    public static void testCreateTable(DBManager dbManager) throws IOException {
        // On suppose que la base "TestDB" est déjà sélectionnée
        List<String> colNames = Arrays.asList("ID", "Name", "Age");
        List<String> colTypes = Arrays.asList("INT", "VARCHAR(10)", "INT");

        dbManager.CreateTable("Persons", colNames, colTypes);
        Relation r = dbManager.GetTableFromCurrentDatabase("Persons");
        assert r != null : "Table 'Persons' should exist!";
        assert r.getName().equals("Persons") : "Table name mismatch!";
        assert r.getColumnCount() == 3 : "Column count mismatch!";
        System.out.println("testCreateTable passed.");
    }

    public static void testListDatabasesTables(DBManager dbManager) {
        // Ajout d'une autre base
        dbManager.CreateDatabase("AnotherDB");
        System.out.println("Databases after creation of AnotherDB:");
        dbManager.ListDatabases();

        // Ajout d'une autre table dans TestDB
        dbManager.SetCurrentDatabase("TestDB");
        try {
            dbManager.CreateTable("Animals", Arrays.asList("AnimalID", "Species"), Arrays.asList("INT", "VARCHAR(10)"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Tables in TestDB:");
        dbManager.ListTablesInCurrentDatabase();
        // On ne fait pas d'assert sur l'affichage, mais on pourrait rediriger la sortie et vérifier.

        System.out.println("testListDatabasesTables passed.");
    }

    public static void testDropTable(DBManager dbManager) throws IOException {
        dbManager.SetCurrentDatabase("TestDB");
        dbManager.DropTable("Animals");
        Relation r = dbManager.GetTableFromCurrentDatabase("Animals");
        assert r == null : "Animals table should be dropped!";
        System.out.println("testDropTable passed.");
    }

    public static void testDropDatabase(DBManager dbManager) throws IOException {
        dbManager.CreateDatabase("TestDB");
        dbManager.SetCurrentDatabase("TestDB");

        dbManager.CreateDatabase("AnotherDB");
        dbManager.SetCurrentDatabase("AnotherDB");
        
        assert dbManager.getCurrentDatabaseName().equals("AnotherDB") : "Should be on AnotherDB now!";

        // La supprimer
        dbManager.RemoveDatabase("AnotherDB");
        assert dbManager.getCurrentDatabaseName() == null : "AnotherDB should be removed and current DB set to null!";

        System.out.println("testDropDatabase passed.");
    }


    public static void testRemoveAllDatabases(DBManager dbManager) throws IOException {
        dbManager.RemoveDatabases();
        assert dbManager.getCurrentDatabaseName() == null : "No current DB after removing all!";
        System.out.println("testRemoveAllDatabases passed.");
    }

    public static void testSaveLoadState(DBManager dbManager) throws IOException, ClassNotFoundException {
        // Recréer quelques bases et tables
        dbManager.CreateDatabase("SavedDB");
        dbManager.SetCurrentDatabase("SavedDB");
        try {
            dbManager.CreateTable("Things", Arrays.asList("ThingID","Desc"), Arrays.asList("INT","VARCHAR(10)"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Sauvegarde de l'état
        dbManager.SaveState();

        // Recréer le manager pour tester le chargement
        DBManager newDbManager = new DBManager(dbManager.getConfig(), dbManager.getDiskManager(), dbManager.getBufferManager());
        newDbManager.LoadState();

        // Vérifier que SavedDB et Things existent
        newDbManager.SetCurrentDatabase("SavedDB");
        Relation r = newDbManager.GetTableFromCurrentDatabase("Things");
        assert r != null : "Things table should be loaded!";
        assert r.getName().equals("Things") : "Table name mismatch after load!";
        System.out.println("testSaveLoadState passed.");
    }
}
