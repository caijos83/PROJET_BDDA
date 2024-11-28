import java.io.*;
import java.nio.*;
import java.util.*;

public class DiskManager {
    private final DBConfig config;
    private List<PageId> freePages;
    private Map<String, List<PageId>> pageTableMapping; // Associe les noms de tables aux pages utilisées

    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new ArrayList<>();
        this.pageTableMapping = new HashMap<>(); // Initialiser les associations table-pages

        // Assurer que le dossier BinData existe
        File binDataDir = new File(config.getDbpath() + "/BinData");
        if (!binDataDir.exists()) {
            binDataDir.mkdirs();
        }
    }

    // Allouer une nouvelle page
    public PageId allocPage(String tableName) throws IOException {
        PageId pageId;

        if (!freePages.isEmpty()) {
            // Récupère une page libre existante
            pageId = freePages.remove(0);
        } else {
            // Alloue une nouvelle page
            int fileIdx = 0;

            if (config.getPagesize() == 0) {
                throw new ArithmeticException("Page size cannot be zero.");
            }

            int pageIdx = (int) (new File(config.getDbpath() + "./Data/BinData/F" + fileIdx + ".rsdb").length() / config.getPagesize());
            pageId = new PageId(fileIdx, pageIdx);
        }

        // Associe la page à la table dans pageTableMapping
        pageTableMapping.computeIfAbsent(tableName, k -> new ArrayList<>()).add(pageId);

        return pageId;
    }

    // Lire une page
    public void readPage(PageId pageId, ByteBuffer buffer) throws IOException {
        File file = new File(config.getDbpath() + "./Data/BinData/F" + pageId.getFileIdx() + ".rsdb");
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek((long) pageId.getPageIdx() * config.getPagesize());
            byte[] data = new byte[config.getPagesize()];
            raf.readFully(data);
            buffer.put(data);
        }
    }

    // Écrire une page
    public void writePage(PageId pageId, ByteBuffer buffer) throws IOException {
        buffer.rewind();

        File file = new File(config.getDbpath() + "./Data/BinData/F" + pageId.getFileIdx() + ".rsdb");
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long position = (long) pageId.getPageIdx() * config.getPagesize();
            raf.seek(position);
            byte[] data = new byte[config.getPagesize()];
            buffer.get(data);
            raf.write(data);
        }
    }

    // Désallouer une page
    public void deallocPage(PageId pageId) {
        freePages.add(pageId);
    }

    // Sauvegarder l'état
    public void saveState() throws IOException {
        File dbDir = new File(config.getDbpath() + "/BinData");
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(config.getDbpath() + "/DB/dm.save"))) {
            out.writeObject(freePages);
            out.writeObject(pageTableMapping); // Sauvegarder les associations table-pages
        }
    }

    // Charger l'état
    @SuppressWarnings("unchecked")
    public void loadState() throws IOException, ClassNotFoundException {
        File saveFile = new File(config.getDbpath() + "/BinData/dm.save");

        if (!saveFile.exists()) {
            System.out.println("No saved state found.");
            return;
        }

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(saveFile))) {
            freePages = (List<PageId>) in.readObject();
            pageTableMapping = (Map<String, List<PageId>>) in.readObject(); // Charger les associations
        }
    }

    // Récupérer les pages associées à une table
    public List<PageId> getPagesForTable(String tableName) {
        return pageTableMapping.getOrDefault(tableName, Collections.emptyList());
    }

    /**
     * Marque une page comme libre en la retirant des associations avec les tables
     * et en l'ajoutant à la liste des pages libres (freePages).
     *
     * @param pageId L'identifiant unique de la page (PageId) à libérer.
     * Fonctionnement :
     * 1. Supprime la page spécifiée de toutes les associations dans `pageTableMapping`,
     *    qui relie les tables aux pages qui leur sont allouées.
     * 2. Vérifie si la page n'est pas déjà dans `freePages`.
     *    - Si elle n'y est pas, elle est ajoutée, marquant ainsi qu'elle est libre pour une future réutilisation.
     *    - Sinon, un message indique que la page est déjà libre.
     */
    public void freePage(PageId pageId) {
        pageTableMapping.values().forEach(pageList -> pageList.remove(pageId));

        if (!freePages.contains(pageId)) {
            freePages.add(pageId);
        }
    }

    // Supprimer une table et ses pages associées
    public void removeTable(String tableName) {
        List<PageId> pages = pageTableMapping.remove(tableName);
        if (pages != null) {
            for (PageId pageId : pages) {
                deallocPage(pageId);
            }
        }
    }
}
