import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;


public class BufferManager {
    /**
     * bufferPool est une map qui associe chaque page (via son PageId) à un buffer de données (ByteBuffer).
     * Lorsqu'une page est demandée, elle est d'abord cherchée dans le bufferPool. Si elle s'y trouve, on
     * la renvoie immédiatement, sans avoir besoin de lire sur le disque.
     * Si la page n'est pas dans le bufferPool, elle doit être lue depuis le disque via
     * le DiskManager et ensuite ajoutée au pool de buffers.
     * */
    private final Map<PageId, ByteBuffer> bufferPool; //  Ce champ est utilisé pour associer des objets de type PageId à des objets de type ByteBuffer
    private DBConfig config;
    private DiskManager diskManager;
    private String replacementPolicy; // LRU ou MRU
    private LinkedList<PageId> lruList;  // Liste pour gérer la politique LRU
    private LinkedList<PageId> mruList;  // Liste pour gérer la politique MRU
    // Ajout d'une Map pour suivre les pages "dirty"
    private Map<PageId, Boolean> dirtyPages = new HashMap<>();

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.replacementPolicy = config.getBm_policy(); // Initialiser avec la politique de remplacement
        this.bufferPool = new HashMap<>();
        lruList = new LinkedList<>();
        mruList = new LinkedList<>();
    }

    // Récupérer une page dans un buffer
    public ByteBuffer GetPage(PageId pageId) throws IOException {
        // Vérifier si la page est déjà dans le buffer
        if (bufferPool.containsKey(pageId)) {
            // La page est en mémoire, on la retourne
            // Si on est en LRU, on met la page à jour dans la liste (la rendre récemment utilisée)
            if ("LRU".equals(replacementPolicy)) {
                lruList.remove(pageId);  // Supprime l'ancienne position
                lruList.addLast(pageId); // Ajoute à la fin, ce qui signifie "récemment utilisée"
            }
            return bufferPool.get(pageId);
        }

        // Si la page n'est pas dans le buffer, vérifier s'il reste de la place
        if (bufferPool.size() >= config.getBmBuffercount()) {
            applyReplacementPolicy(); // Remplacer une page si le buffer est plein
        }

        // Charger la page depuis le disque
        ByteBuffer buff = ByteBuffer.allocate(config.getPagesize());
        diskManager.ReadPage(pageId, buff);

        // Ajouter la page au buffer pool et mettre à jour les listes LRU/MRU
        bufferPool.put(pageId, buff);

        // Ajouter la page dans les listes selon la politique
        if ("LRU".equals(replacementPolicy)) {
            lruList.addLast(pageId); // La page est récemment utilisée
        } else if ("MRU".equals(replacementPolicy)) {
            mruList.addFirst(pageId); // La page est la plus récemment utilisée
        }

        // Retourner le buffer contenant la page
        return buff;
    }

    // Libérer une page dans le buffer
    public void FreePage(PageId pageId, boolean valdirty) {
        if (valdirty) {
            setDirty(pageId, true);
        }
    }

    private void applyReplacementPolicy() throws IOException {
        PageId pageToEvict;

        if ("LRU".equals(replacementPolicy)) {
            // Supprimer la page la moins récemment utilisée (première dans la liste LRU)
            pageToEvict = lruList.removeFirst();
        } else if ("MRU".equals(replacementPolicy)) {
            // Supprimer la page la plus récemment utilisée (première dans la liste MRU)
            pageToEvict = mruList.removeFirst();
        } else {
            throw new IllegalStateException("Politique de remplacement inconnue: " + replacementPolicy);
        }

        // Si la page est dirty, on l'écrit sur le disque avant de l'évincer
        ByteBuffer bufferToEvict = bufferPool.get(pageToEvict);
        if (bufferToEvict != null && isDirty(pageToEvict)) {
            diskManager.WritePage(pageToEvict, bufferToEvict);
        }

        // Retirer la page du bufferPool
        bufferPool.remove(pageToEvict);
    }

    // Changer la politique de remplacement
    public void SetCurrentReplacementPolicy(String policy) {
        this.replacementPolicy = policy;
        System.out.println("Politique de remplacement mise à jour : " + this.replacementPolicy);
    }

    // Vider tous les buffers et écrire les pages modifiées sur disque
    public void FlushBuffers() throws IOException {
        for (Map.Entry<PageId, ByteBuffer> entry : bufferPool.entrySet()) {
            PageId pageId = entry.getKey();
            ByteBuffer buff = entry.getValue();
            // Si la page est dirty, écrire sur le disque
            diskManager.WritePage(pageId, buff);
        }
        bufferPool.clear(); // Réinitialiser tous les buffers
    }

    // Méthode pour marquer une page comme dirty
    private void setDirty(PageId pageId, boolean dirty) {
        dirtyPages.put(pageId, dirty);
    }

    // Méthode pour vérifier si une page est dirty
    private boolean isDirty(PageId pageId) {
        return dirtyPages.getOrDefault(pageId, false);
    }
}

