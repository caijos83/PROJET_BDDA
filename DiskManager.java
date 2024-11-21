import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class DiskManager {
    private final DBConfig config;
    private List<PageId> freePages;
    private Map<String, List<PageId>> pageTableMapping; // Associe les noms de tables aux pages utilisées


    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new ArrayList<>();
        pageTableMapping = new HashMap<>(); // Initialise la map
    }


    public PageId AllocPage(String tableName) {
        PageId pageId;

        if (!freePages.isEmpty()) {
            // Récupère une page libre existante
            pageId = freePages.remove(0);
        } else {
            // Alloue une nouvelle page
            int fileIdx = 0; // Exemple, il faut ajuster selon la logique du fichier courant

            if (config.getPagesize() == 0) {
                throw new ArithmeticException("pagesize ne peut pas être zéro");
            }

            int pageIdx = (int) (new File(config.getDbpath() + "/F" + fileIdx + ".rsdb").length() / config.getPagesize());
            pageId = new PageId(fileIdx, pageIdx);
        }

        // Associe la page à la table dans pageTableMapping
        pageTableMapping.computeIfAbsent(tableName, k -> new ArrayList<>()).add(pageId);

        return pageId;
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
    public void LoadState() throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(config.getDbpath() + "/dm.save"));
        freePages = (List<PageId>) in.readObject();
        in.close();
    }

    public List<PageId> GetPagesForTable(String tableName) {
        // retourne une liste des pages associées
        return pageTableMapping.getOrDefault(tableName, Collections.emptyList()); // Où pageTableMapping est une Map<String, List<PageId>>
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
    public void FreePage(PageId pageId) {
        // Supprime la page des associations avec les tables
        pageTableMapping.values().forEach(pageList -> pageList.remove(pageId));

        // Ajoute la page à la liste des pages libres
        if (!freePages.contains(pageId)) {
            freePages.add(pageId);
            System.out.println("Page " + pageId + " freed.");
        } else {
            System.out.println("Page " + pageId + " is already in the free list.");
        }
    }

}
