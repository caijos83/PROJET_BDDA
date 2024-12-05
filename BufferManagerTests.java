package miniSGBDR;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferManagerTests {
    public static void main(String[] args) throws IOException {
        // Configuration de la base de données
        DBConfig config = new DBConfig(".\\Data", 4096, 22288, DBConfig.BMpolicy.LRU, 3);
        DiskManager diskManager = new DiskManager(config);
        BufferManager bufferManager = new BufferManager(config, diskManager);

        // Lancer les tests
        testGetPage(bufferManager);
        testMarkPageDirty(bufferManager);
        testFlushBuffers(bufferManager);
        testReplacementPolicy(bufferManager);

        System.out.println("Tous les tests sont terminés avec succès !");
    }

    /**
     * Teste le chargement d'une page dans le buffer.
     */
    public static void testGetPage(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        ByteBuffer buffer = bufferManager.getPage(pageId);

        System.out.println("testGetPage passed: " + (buffer != null));
        assert buffer != null : "Échec : Impossible de charger la page dans le buffer.";
    }


    /**
     * Teste le marquage d'une page comme modifiée (dirty).
     */
    public static void testMarkPageDirty(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);

        // Charger la page avant de la marquer comme dirty
        bufferManager.getPage(pageId);
        bufferManager.freePage(pageId, true); // Libère et marque comme "dirty"

        // Vérifier si la page est bien marquée comme modifiée
        Boolean result = bufferManager.getFlagDirty(pageId);
        System.out.println("testMarkPageDirty passed: " + (result));
        assert result : "Échec : Impossible de marquer la page comme modifiée.";
    }


    /**
     * Teste le vidage du buffer (flush) en écrivant les pages modifiées sur disque.
     */
    public static void testFlushBuffers(BufferManager bufferManager) throws IOException {
        PageId pageId1 = new PageId(0, 1);
        PageId pageId2 = new PageId(0, 2);
        PageId pageId3 = new PageId(0, 3);

        // Allouer explicitement les pages pour s'assurer qu'elles existent
        DiskManager diskManager = bufferManager.getDiskManager();
        diskManager.allocPage();
        diskManager.allocPage();
        diskManager.allocPage();

        // Charger les pages dans le buffer
        bufferManager.getPage(pageId1);
        bufferManager.getPage(pageId2);
        bufferManager.getPage(pageId3);

        // Marquer une page comme "dirty"
        bufferManager.freePage(pageId2, true);

        // Flusher les buffers
        bufferManager.flushBuffers();

        // Vérifier si les pages modifiées ont été écrites sur disque
        System.out.println("testFlushBuffers passed: " + 
            (bufferManager.getPinCount().isEmpty() && 
            bufferManager.getFlagDirtyMap().isEmpty() && 
            bufferManager.getBufferPool().isEmpty() ));


        assert bufferManager.getPinCount().isEmpty() : "Échec : Le pinCountMap n'est pas vide après le flush.";
        assert bufferManager.getFlagDirtyMap().isEmpty() : "Échec : Le flagDirtyMap n'est pas vide après le flush.";
        assert bufferManager.getBufferPool().isEmpty() : "Échec : Le bufferPool n'est pas vide après le flush.";


    }

    /**
     * Teste la politique de remplacement (LRU/MRU) dans le buffer.
     */
    public static void testReplacementPolicy(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        PageId pageId2 = new PageId(0, 2);
        PageId pageId3 = new PageId(0, 3);
        PageId pageId4 = new PageId(0, 4); // Déclenchera une éviction

        // Charger des pages jusqu'à dépasser la capacité du buffer
        bufferManager.getPage(pageId);
        bufferManager.getPage(pageId2);
        bufferManager.getPage(pageId3);
        bufferManager.getPage(pageId4); // Éviction de la première page (LRU)

        // Vérifier si la première page a été évincée
        Boolean isLoaded = bufferManager.isLoad(pageId); // Devrait être false si évincée
        System.out.println("testReplacementPolicy passed: " + (!isLoaded));
        assert !isLoaded : "Échec : La politique de remplacement n'a pas fonctionné correctement.";
    }
}
