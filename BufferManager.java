import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {
    private DBConfig config;
    private DiskManager diskManager;
    private Map<PageId, ByteBuffer> bufferPool; // Contient les pages chargées
    private String replacementPolicy; // LRU ou MRU

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.replacementPolicy = config.getBm_policy(); // Initialiser avec la politique de remplacement
        this.bufferPool = new HashMap<>(); // Pool de buffers
    }

    // Récupérer une page dans un buffer
    public ByteBuffer GetPage(PageId pageId) throws IOException {
        // Si la page est déjà dans le buffer
        if (bufferPool.containsKey(pageId)) {
            return bufferPool.get(pageId);
        }

        // Si la page n'est pas dans le buffer, on la charge depuis le disque
        ByteBuffer buff = ByteBuffer.allocate(config.getPagesize());
        diskManager.ReadPage(pageId, buff);

        // Appliquer la politique de remplacement si nécessaire
        if (bufferPool.size() >= config.getBm_buffercount()) {
            applyReplacementPolicy();
        }

        // Ajouter la page dans le buffer pool
        bufferPool.put(pageId, buff);
        return buff;
    }

    // Libérer une page dans le buffer
    public void FreePage(PageId pageId, boolean dirty) {
        // Mettre à jour le flag dirty et décrémenter le pin_count (non montré ici)
        // Pas d'appel au DiskManager ici
    }

    // Appliquer la politique de remplacement (LRU/MRU)
    private void applyReplacementPolicy() {
        if ("LRU".equals(replacementPolicy)) {
            // Appliquer la politique LRU pour remplacer la page
        } else if ("MRU".equals(replacementPolicy)) {
            // Appliquer la politique MRU pour remplacer la page
        }
    }

    // Changer la politique de remplacement
    public void SetCurrentReplacementPolicy(String policy) {
        this.replacementPolicy = policy;
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
}

