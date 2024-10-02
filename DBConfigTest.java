import java.io.IOException;

public class DBConfigTest {
    public static void main(String[] args) {
        // Création manuelle d'une instance de DBConfig
        DBConfig config = new DBConfig("../DB");
        System.out.println("Chemin défini manuellement");
        config.printConfig();  // Affiche le chemin de la DB

        // Création via un fichier texte
        try {
            DBConfig configFromFile = DBConfig.LoadDBConfig("config.txt");
            System.out.println("Chemnin lu à partir du fichier config.txt" );
            configFromFile.printConfig();
        } catch (IOException e) {
            System.out.println("Erreur lors du chargement de la configuration : " + e.getMessage());
        }
    }
}
