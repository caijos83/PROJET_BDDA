// package projet_SGBD;
import java.io.IOException;

public class TestDBConfig {
    public static void main(String[] args) {
        // Création manuelle d'une instance de DBConfig
        // Exemple avec pagesize = 4096 et dm_maxfilesize = 10 Mo
        DBConfig config = new DBConfig("../DB", 4096, 10485760,"LRU",4);
        System.out.println("Chemin défini manuellement");
        config.printConfig();  // Affiche le chemin de la DB

        // Création via un fichier texte
        try {
            DBConfig configFromFile = DBConfig.loadDBConfig("config.txt");
            System.out.println("Chemnin lu à partir du fichier config.txt" );
            configFromFile.printConfig();
        } catch (IOException e) {
            System.out.println("Erreur lors du chargement de la configuration : " + e.getMessage());
        }
    }
}
