
import java.io.*;
import java.nio.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiskManager {
    private  DBConfig config;
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
    
    
   public DBConfig getConf() {
	   return this.config;
   }


 
    /**
     * Alloue une nouvelle page dans le fichier correspondant.
     * @return PageId représentant la page nouvellement allouée.
     */
// Allouer une nouvelle page
public PageId allocPage(String tableName) throws IOException {
    PageId pageId;

    if (!freePages.isEmpty()) {
        return freePages.remove(0);
    } else {
        File binDataDir = new File(config.getDbpath() + "/BinData");
        File[] files = binDataDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        if (files != null) {
            for (File file : files) {
                if (file.length() + config.getPagesize() <= config.getDm_maxfilesize()) {
                    try (FileOutputStream fos = new FileOutputStream(file, true)) {
                        fos.write(new byte[config.getPagesize()]);
                        pageId = new PageId(extractFileIndex(file.getName()), getNextPageIndex(file));

                        // Associe la page à la table dans pageTableMapping
                        pageTableMapping.computeIfAbsent(tableName, k -> new ArrayList<>()).add(pageId);

                        return pageId;
                    }
                }
            }
        }

        // Créer un nouveau fichier si aucun fichier existant ne peut contenir la page
        int nextFileIndex = getNextFileIndex(files);
        File newFile = new File(config.getDbpath() + "/BinData/F" + nextFileIndex + ".rsdb");
        if (newFile.createNewFile()) {
            try (FileOutputStream fos = new FileOutputStream(newFile)) {
                fos.write(new byte[config.getPagesize()]);
                pageId = new PageId(nextFileIndex, 0);

                // Associe la page à la table dans pageTableMapping
                pageTableMapping.computeIfAbsent(tableName, k -> new ArrayList<>()).add(pageId);

                return pageId;
            }
        }

        throw new IOException("Impossible d'allouer une nouvelle page.");
    }

}
    /**
     * Lit une page spécifique dans un buffer.
     */
    public void readPage(PageId pageId, ByteBuffer buffer) throws IOException {
        File file = new File(config.getDbpath() + "/BinData/F" + pageId.getFileIdx() + ".rsdb");
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek((long) pageId.getPageIdx() * config.getPagesize());
            byte[] data = new byte[config.getPagesize()];
            raf.readFully(data);
            buffer.put(data);
        }
    }
    
    /**
     * Écrit une page dans un fichier spécifique.
     */
    public void writePage(PageId pageId, ByteBuffer buffer) throws IOException {
        // Réinitialiser la position pour commencer à lire depuis le début du buffer
        buffer.rewind();

        // Si la taille restante est inférieure à la taille d'une page, compléter avec des zéros
        if (buffer.remaining() < config.getPagesize()) {
            // Compléter les données manquantes pour éviter le BufferUnderflowException
            ByteBuffer tempBuffer = ByteBuffer.allocate(config.getPagesize());
            tempBuffer.put(buffer); // Copier les données existantes
            tempBuffer.position(0); // Réinitialiser la position
            buffer = tempBuffer; // Remplacer par le buffer complété
        }

        // Préparer le fichier pour écrire à la position correcte
        File file = new File(config.getDbpath() + "/BinData/F" + pageId.getFileIdx() + ".rsdb");
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long position = (long) pageId.getPageIdx() * config.getPagesize();
            raf.seek(position);

            // Lire exactement une page depuis le buffer et l'écrire dans le fichier
            byte[] data = new byte[config.getPagesize()];
            buffer.get(data); // Lire exactement une page du buffer
            raf.write(data);
        }
    }
  

    private int extractFileIndex(String fileName) {
        return Integer.parseInt(fileName.substring(1, fileName.indexOf('.')));
    }

    private int getNextPageIndex(File file) {
        return (int) (file.length() / config.getPagesize());
    }

    private int getNextFileIndex(File[] fichiers) {
        int maxIndex = -1;
        if (fichiers != null) {
            for (File fichier : fichiers) {
                int index = extractFileIndex(fichier.getName());
                if (index > maxIndex) {
                    maxIndex = index;
                }
            }
        }
        return maxIndex + 1;
    }


    public void deallocPage(PageId pageId) {
        File dbDir = new File(config.getDbpath() + "/BinData");
        File[] fichiers = dbDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        boolean pageFound = false; 

        if (fichiers != null) {
            for (File fichier : fichiers) {
                if (extractFileIndex(fichier.getName()) == pageId.getFileIdx()) {
                    freePages.add(pageId); 
                    pageFound = true; 
                    System.out.println("Page " + pageId + " désallouée.");
                    break; 
                }
            }
        }

        if (!pageFound) {
            System.err.println("Aucune page trouvée pour la désallocation : " + pageId);
        }
    }

    public void saveState() throws IOException {
        File dbDir = new File(config.getDbpath() + "/BinData");
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(config.getDbpath() + "/BinData/dm.save"))) {
            out.writeObject(freePages);
            out.writeObject(pageTableMapping); // Sauvegarder les associations table-pages
        }
    }



    @SuppressWarnings("unchecked")
    public void loadState() throws IOException, ClassNotFoundException {
        File saveFile = new File(config.getDbpath() + "/BinData/dm.save");

        if (!saveFile.exists()) {
            System.out.println("Aucun état sauvegardé trouvé.");
            return;
        }

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(config.getDbpath() + "/BinData/dm.save"))){
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
