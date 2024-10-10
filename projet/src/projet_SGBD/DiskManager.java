//package projet_SGBD;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DiskManager {
    private final DBConfig config;
    private List<PageId> freePages;


    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new ArrayList<>();
    }


    public PageId AllocPage() {
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        } else {
            // Ajouter une nouvelle page
            int fileIdx = 0; // Exemple, il faut ajuster selon la logique du fichier courant

            if (config.getPagesize() == 0) {
                throw new ArithmeticException("pagesize ne peut pas être zéro");
            }

            int pageIdx = (int) (new File(config.getDbpath() + "/F" + fileIdx + ".rsdb").length() / config.getPagesize());
            return new PageId(fileIdx, pageIdx);
        }
    }

    // Lire une page
    public void ReadPage(PageId pageId, ByteBuffer buff) throws IOException {
        RandomAccessFile file = new RandomAccessFile(config.getDbpath() + "/F" + pageId.getFileIdx() + ".rsdb", "r");
        file.seek((long) pageId.getPageIdx() * config.getPagesize());
        byte[] bytes = new byte[config.getPagesize()];
        file.readFully(bytes);
        buff.put(bytes);
        file.close();
    }

    // Écrire une page
    public void WritePage(PageId pageId, ByteBuffer buff) throws IOException {
        RandomAccessFile file = new RandomAccessFile(config.getDbpath() + "/F" + pageId.getFileIdx() + ".rsdb", "rw");
        file.seek((long) pageId.getPageIdx() * config.getPagesize());
//        byte[] bytes = new byte[config.getPagesize()];
//        buff.get(bytes);
        file.write(buff.array());
        file.close();
    }

    // Désallouer une page
    public void DeallocPage(PageId pageId) {
        freePages.add(pageId); // Ajouter à la liste des pages libres
    }

    // Sauvegarder l'état du DiskManager
    public void SaveState() throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(config.getDbpath() + "/dm.save"));
        out.writeObject(freePages);
        out.close();
    }

    // Charger l'état du DiskManager
    @SuppressWarnings("unchecked")
    public void LoadState() throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(config.getDbpath() + "/dm.save"));
        freePages = (List<PageId>) in.readObject();
        in.close();
    }
}
