import java.nio.ByteBuffer;

public class BufferManagerTest {
    public static void main(String[] args) throws Exception {
        DBConfig config = DBConfig.LoadDBConfig("config.txt");
        DiskManager diskManager = new DiskManager(config);
        BufferManager bufferManager = new BufferManager(config, diskManager);

        // Test de récupération d'une page
        PageId pageId = new PageId(0, 0);
        ByteBuffer buffer = bufferManager.GetPage(pageId);
        System.out.println("Page récupérée dans le buffer: " + pageId);

        // Test de libération d'une page
        bufferManager.FreePage(pageId, true);

        // Test de changement de politique de remplacement
        bufferManager.SetCurrentReplacementPolicy("MRU");
        System.out.println("Politique de remplacement actuelle: " + config.getBmPolicy());

        // Test de vidage des buffers
        bufferManager.FlushBuffers();
        System.out.println("Buffers vidés et pages écrites sur disque.");
    }
}
