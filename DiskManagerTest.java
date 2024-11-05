import java.nio.ByteBuffer;

public class DiskManagerTest {

    public static void main(String[] args) throws Exception {
        DBConfig config = DBConfig.loadDBConfig("config.txt");
        DiskManager dm = new DiskManager(config);

        // Test allocation de page
        PageId pageId = dm.allocPage();
        System.out.println("Page allouée: " + pageId);


        ByteBuffer buff = ByteBuffer.allocate(config.getPagesize());
        buff.put("Hello, World!".getBytes());
        System.out.println(buff.position() + " " + buff.capacity());
        buff.flip();
        dm.writePage(pageId, buff);


        ByteBuffer readBuff = ByteBuffer.allocate(config.getPagesize());
        dm.readPage(pageId, readBuff);
        readBuff.flip();
        System.out.println("Contenu lu: " + new String(readBuff.array()));

        // Test désallocation de page
        dm.deallocPage(pageId);
        System.out.println("Page désallouée: " + pageId);

        // Test sauvegarde de l'état
        dm.SaveState();
        System.out.println("État sauvegardé");

        // Test chargement de l'état""
        dm.loadState();
        System.out.println("État chargé");
    }
}
