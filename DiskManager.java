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
    }
   public DBConfig getConf() {
	   return this.config;
   }


    public PageId allocPage() throws IOException {
        // Si on a des pages libres, les utiliser en priorité
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        }

        // Parcourir les fichiers existants pour trouver de l’espace
        File dbDir = new File(config.getDbpath());
        // crée un tableau de page contenant les fichier se terminant par l'extension rsdb
        File[] fichiers = dbDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        if (fichiers != null) {
            for (File fichier : fichiers) {
                // Si le fichier a encore de l’espace, on y ajoute une page
                if (fichier.length() + config.getPagesize() <= config.getDm_maxfilesize()) {
                    try (FileOutputStream fos = new FileOutputStream(fichier, true)) {
                        fos.write(new byte[config.getPagesize()]);  // Ajoute une page vide
                        return new PageId(extractFileIndex(fichier.getName()), getNextPageIndex(fichier));
                    } catch (IOException e) {
                        System.err.println("Erreur lors de l'écriture dans le fichier " + fichier.getName() + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }

        // Si tous les fichiers sont pleins, créer un nouveau fichier
        int nextFileIndex = getNextFileIndex(fichiers);
        File newFile = new File(config.getDbpath() + "/F" + nextFileIndex + ".rsdb");
        try {
            if (newFile.createNewFile()) {
                try (FileOutputStream fos = new FileOutputStream(newFile)) {
                    fos.write(new byte[config.getPagesize()]);  // Ajoute une première page vide
                    return new PageId(nextFileIndex, 0);  // Première page du nouveau fichier
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la création du fichier : " + e.getMessage());
            e.printStackTrace();
        }

        // 5. En cas d’échec complet, retourner null ou lever une exception personnalisée
        throw new IOException("Impossible d'allouer une page.");
    }

    // Méthode pour extraire l'index d'un fichier à partir de son nom
    private int extractFileIndex(String fileName) {
        // Extrait l'index du fichier en prenant tout ce qu’il y a entre le F et le point. On le convertir en entier car c'est un caractère
        return Integer.parseInt(fileName.substring(1, fileName.indexOf('.')));
    }

    // Méthode pour calculer l'index de la prochaine (nouvelle) page d'un fichier
    private int getNextPageIndex(File file) {
        // Le prochain index disponible pour une nouvelle page est le nombre de pages exacte du fichier (car les index commencent à 0).
        return (int) (file.length() / config.getPagesize());
    }

    // Méthode pour déterminer le prochain index de fichier disponible
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



    // Lire une page
    public void readPage(PageId pageId, ByteBuffer buff) throws IOException {
        // Vérification de la capacité du ByteBuffer
        if (buff.remaining() < config.getPagesize()) {
            throw new BufferOverflowException(); // Ou lever l'exception le buffer est plein
        }

        // Utilisation de try-with-resources pour garantir la fermeture du fichier
        try (RandomAccessFile file = new RandomAccessFile(config.getDbpath() + "/F" + pageId.getFileIdx() + ".rsdb", "r")) {
            file.seek((long) pageId.getPageIdx() * config.getPagesize());
            byte[] bytes = new byte[config.getPagesize()];
            file.readFully(bytes);
            buff.put(bytes); // Ajoute les octets au ByteBuffer
        } catch (IOException e) {
            throw new IOException("Erreur lors de la lecture de la page " + pageId, e);
        }
    }


    // Écrire une page
    public void writePage(PageId pageId, ByteBuffer buff) throws IOException {
        
        // Vérification de la capacité du ByteBuffer
    	//if (buff.remaining() < config.getPagesize()) {
           // throw new BufferOverflowException(); // Lever l'exception si le buffer est plein
        //}

        try (RandomAccessFile file = new RandomAccessFile(config.getDbpath()+ "/F" + pageId.getFileIdx() + ".rsdb", "rw")) {
            file.seek((long) pageId.getPageIdx() * config.getPagesize());
            byte[] bytes = new byte[config.getPagesize()];
            buff.get(bytes); // Lire les données du ByteBuffer

            // Écrire les octets dans le fichier
            file.write(bytes);
        } catch (IOException e) {
            throw new IOException("Erreur lors de l'écriture de la page " + pageId, e);
        }
    }

    // Désallouer une page
    public void deallocPage(PageId pageId) {
        // Parcourir les fichiers existants pour trouver celui correspondant à pageId
        File dbDir = new File(config.getDbpath());
        File[] fichiers = dbDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        boolean pageFound = false; // Indicateur pour savoir si la page a été trouvée

        if (fichiers != null) {
            for (File fichier : fichiers) {
                // Vérifie si le fichier correspond à celui du pageId
                if (extractFileIndex(fichier.getName()) == pageId.getFileIdx()) {
                    // Ici, on pourrait envisager de marquer la page dans le fichier ou gérer d'autres opérations
                    // Actuellement, nous ajoutons simplement le PageId à la liste des pages libres

                    freePages.add(pageId); // Ajoute le PageId à la liste des pages libres
                    pageFound = true; // Marque que la page a été trouvée et désallouée

                    System.out.println("Page " + pageId + " désallouée.");
                    break; // Sort de la boucle après avoir désalloué la page
                }
            }
        }

        // Gérer le cas où la page n'a pas été trouvée
        if (!pageFound) {
            System.err.println("Aucune page trouvée pour la désallocation : " + pageId);
        }
    }

    // Sauvegarder l'état du DiskManager
    public void saveState() throws IOException {
        File dbDir = new File(config.getDbpath()); // Créer une instance File pour le dossier
        if (!dbDir.exists()) {
            dbDir.mkdirs();  // Créer le dossier s'il est inexistant
        }

        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(config.getDbpath() + "/dm.save"))) {
            out.writeObject(freePages);  // Sérialiser et écrire l'objet freePages dans le fichier
        } // Flux automatiquement fermé ici grace au try-with-ressources
    }



    // Charger l'état du DiskManager
    @SuppressWarnings("unchecked")
    public void loadState() throws IOException, ClassNotFoundException {
        // On commence par vérifier si le fichier que l'on veut restaurer est au préalable existant
        File saveFile = new File(config.getDbpath() + "/dm.save");

        if (!saveFile.exists()) {
            System.out.println("Aucun état sauvegardé trouvé.");
            return;  // Si le fichier n'existe pas, sortir de la méthode
        }
        // S'il existe bien, on restaure ses pages libres :
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(config.getDbpath() + "/dm.save"))){
            freePages = (List<PageId>) in.readObject();
        } // Flux automatiquement fermé ici grace au try-with-ressources
    }
}