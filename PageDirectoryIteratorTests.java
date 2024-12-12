package tests;
import miniSGBDR.*;

import java.nio.ByteBuffer;

public class PageDirectoryIteratorTests {
    public static void main(String[] args) throws Exception {
        DBConfig dbConfig = new DBConfig(".\\Data", 4096, 20480,
                DBConfig.BMpolicy.LRU, 5);
        DiskManager diskManager = new DiskManager(dbConfig);
        BufferManager bufferManager = new BufferManager(dbConfig, diskManager);
        PageId headerPage = diskManager.allocPage();

        // Initialiser le contenu de la Header Page
        ByteBuffer headerBuffer = bufferManager.getPage(headerPage);
        headerBuffer.putInt(0, 2); // 2 pages de données à suivre

        // Simuler deux entrées dans le Page Directory
        // Première entrée : fileId = 0, page = 1
        headerBuffer.putInt(4, 0);   // fileId
        headerBuffer.putInt(8, 1);   // pageNumber
        headerBuffer.putInt(12, 4000); // espace libre (exemple)

        // Deuxième entrée : fileId = 0, page = 2
        headerBuffer.putInt(16, 0);   // fileId
        headerBuffer.putInt(20, 2);   // pageNumber
        headerBuffer.putInt(24, 3500); // espace libre (exemple)

        // Libérer le buffer après modification
        bufferManager.freePage(headerPage, true);
        PageDirectoryIterator pageDirectoryIterator = new PageDirectoryIterator(bufferManager, headerPage);

        // Essai du test
        testGetNextDataPageId(pageDirectoryIterator);


    }
    public static void testGetNextDataPageId(PageDirectoryIterator pDi){
        try {
            // Première page attendue
            PageId pageId1 = pDi.getNextDataPageId();
            boolean test1;
            if (pageId1 != null && pageId1.getFileIdx() == 0 && pageId1.getPageIdx() == 1) {
                test1 = true;
                System.out.println("Test 1 réussi : Première page récupérée correctement.");
            } else {
                test1 = false;
                System.out.println("Test 1 échoué : Mauvaise première page.");
            }

            // Deuxième page attendue
            PageId pageId2 = pDi.getNextDataPageId();
            boolean test2;
            if (pageId2 != null && pageId2.getFileIdx() == 0 && pageId2.getPageIdx() == 2) {
                test2 = true;
                System.out.println("Test 2 réussi : Deuxième page récupérée correctement.");
            } else {
                test2 = false;
                System.out.println("Test 2 échoué : Mauvaise deuxième page.");
            }

            // Plus de pages disponibles
            boolean test3;
            PageId pageId3 = pDi.getNextDataPageId();
            if (pageId3 == null) {
                test3 = true;
                System.out.println("Test 3 réussi : Aucune page supplémentaire disponible.");
            } else {
                test3 = false;
                System.out.println("Test 3 échoué : Une page supplémentaire inattendue a été retournée.");
            }
            System.out.println("test GetNextDataPageId passed : "+((test1)&&(test2)&&(test3)));

        } catch (Exception e) {
            System.out.println("Erreur lors du test : " + e.getMessage());
        }
    }




}
