import java.nio.ByteBuffer;

public class DiskManagerTest {

    public static void main(String[] args) throws Exception {
        DBConfig config = DBConfig.LoadDBConfig("config.txt");
        DiskManager dm = new DiskManager(config);

        // Test allocation de page
        PageId pageId = dm.AllocPage();
        System.out.println("Page allouée: " + pageId);

        // Test écriture de page
        ByteBuffer buff = ByteBuffer.allocate(config.getPagesize());
        buff.put("Hello, World!".getBytes());
        buff.flip();
        dm.WritePage(pageId, buff);

        // Test lecture de page
        ByteBuffer readBuff = ByteBuffer.allocate(config.getPagesize());
        dm.ReadPage(pageId, readBuff);
        readBuff.flip();
        System.out.println("Contenu lu: " + new String(readBuff.array()));

        // Test désallocation de page
        dm.DeallocPage(pageId);
        System.out.println("Page désallouée: " + pageId);

        // Test sauvegarde de l'état
        dm.SaveState();
        System.out.println("État sauvegardé");

        // Test chargement de l'état
        dm.LoadState();
        System.out.println("État chargé");
    }
}
