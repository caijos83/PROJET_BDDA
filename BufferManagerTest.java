import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferManagerTest {

    public static void main(String[] args) throws Exception {
        // Charger la configuration avec une petite taille de page et un nombre de buffers limité
        DBConfig config = DBConfig.loadDBConfig("./config.txt");
        /*
        config.setPagesize(128); 
        config.setBm_buffercount(3); // Limite à 3 buffers dans le pool
        config.setBm_policy("LRU"); // Politique de remplacement par défaut
         */
        // Créer un DiskManager simulé
        DiskManager diskManager = new DiskManager(config);

        // Créer le BufferManager
        BufferManager bufferManager = new BufferManager(config, diskManager);

        // Créer et allouer quelques pages
        PageId page1 = diskManager.allocPage();
        PageId page2 = diskManager.allocPage();
        PageId page3 = diskManager.allocPage();
        PageId page4 = diskManager.allocPage(); // Cela forcera la politique de remplacement

        // Charger les pages dans le buffer
        System.out.println("Chargement de la page 1...");
        bufferManager.getPage(page1);
        System.out.println("Chargement de la page 2...");
        bufferManager.getPage(page2);
        System.out.println("Chargement de la page 3...");
        bufferManager.getPage(page3);

        // Le pool de buffers est maintenant plein, la prochaine page doit déclencher la politique de remplacement
        System.out.println("Chargement de la page 4 (devrait déclencher la politique de remplacement)...");
        bufferManager.getPage(page4);

        // Test de la politique LRU : page1 devrait être évincée (car elle est la moins récemment utilisée)
        System.out.println("Recharger la page 1 (devrait être rechargée depuis le disque)...");
        bufferManager.getPage(page1);

        // Tester la politique MRU
        System.out.println("Changement de la politique de remplacement à MRU...");
        bufferManager.setCurrentReplacementPolicy(DBConfig.BMpolicy.MRU);

        // Charger de nouvelles pages et observer le remplacement selon la nouvelle politique
        System.out.println("Chargement de la page 2...");
        bufferManager.getPage(page2);  // Remplacer la page la plus récemment utilisée (MRU)

        System.out.println("Chargement de la page 3...");
        bufferManager.getPage(page3);  // Remplacer encore selon MRU

        // Vider tous les buffers et écrire les pages modifiées sur disque
        System.out.println("Flush des buffers...");
        bufferManager.flushBuffers();

        System.out.println("Test terminé.");
    }
}
