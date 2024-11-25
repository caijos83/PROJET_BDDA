package miniSGBDR;

import java.io.*;
import java.nio.*;
import java.util.ArrayList;
import java.util.List;

public class DiskManager {
    private  DBConfig config;
    private List<PageId> freePages;


    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new ArrayList<>();

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
    public PageId allocPage() throws IOException {
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        }

        File binDataDir = new File(config.getDbpath() + "/BinData");
        File[] files = binDataDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        if (files != null) {
            for (File file : files) {
                if (file.length() + config.getPagesize() <= config.getDm_maxfilesize()) {
                    try (FileOutputStream fos = new FileOutputStream(file, true)) {
                        fos.write(new byte[config.getPagesize()]);
                        return new PageId(extractFileIndex(file.getName()), getNextPageIndex(file));
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
                return new PageId(nextFileIndex, 0);
            }
        }

        throw new IOException("Impossible d'allouer une nouvelle page.");
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
        }
    }
}
