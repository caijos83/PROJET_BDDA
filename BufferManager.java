package miniSGBDR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {
    private final DBConfig dbConfig; // Configuration globale
    private final DiskManager diskManager; // Gestionnaire de disque
    private final Map<PageId, ByteBuffer> bufferPool; // Pool de buffers
    private final Map<PageId, Boolean> flag_dirty; // Indicateurs de pages modifiées
    private final Map<PageId, Integer> pin_count; // Nombre d'utilisations (pin) par page

    // Constructeur
    public BufferManager(DBConfig dbConfig, DiskManager diskManager) {
        this.dbConfig = dbConfig;
        this.diskManager = diskManager;
        this.bufferPool = new HashMap<>();
        this.flag_dirty = new HashMap<>();
        this.pin_count = new HashMap<>();
    }

    /**
     * Récupère une page du pool ou la charge depuis le disque si nécessaire.
     * @param pageId Identifiant de la page
     * @return Buffer contenant la page
     * @throws IOException En cas d'erreur de lecture
     */
    public ByteBuffer getPage(PageId pageId) throws IOException {
        ByteBuffer buffer = bufferPool.get(pageId);

        if (buffer == null) { // Si la page n'est pas déjà en mémoire
            buffer = ByteBuffer.allocate(dbConfig.getPagesize());
            getDiskManager().readPage(pageId, buffer);

            // Appliquer la politique de remplacement si nécessaire
            applyReplacementPolicy(pageId, buffer);
        }

        // Met à jour le compteur de pins
        pin_count.put(pageId, pin_count.getOrDefault(pageId, 0) + 1);
        return buffer;
    }

    /**
     * Applique la politique de remplacement pour insérer une nouvelle page.
     * Si le pool est plein, évince une page selon la politique choisie.
     * @param pageId Identifiant de la nouvelle page à insérer
     * @param buffer Buffer contenant la page
     * @throws IOException En cas d'erreur lors de l'écriture d'une page évincée
     */
    private void applyReplacementPolicy(PageId pageId, ByteBuffer buffer) throws IOException {
        if (bufferPool.size() >= dbConfig.getBm_buffercount()) {
            PageId pageToEvict = findPageToEvict(); // Trouver la page à évincer
            if (pageToEvict != null) {
                ByteBuffer evictedBuffer = bufferPool.remove(pageToEvict);

                // Si la page évincée est modifiée, l'écrire sur disque
                if (Boolean.TRUE.equals(flag_dirty.get(pageToEvict))) {
                    getDiskManager().writePage(pageToEvict, evictedBuffer);
                }

                // Nettoyage des métadonnées
                flag_dirty.remove(pageToEvict);
                pin_count.remove(pageToEvict);
            }
        }

        // Insérer la nouvelle page dans le pool
        bufferPool.put(pageId, buffer);
        pin_count.put(pageId, 1); // Nouvelle page, utilisée une fois
        flag_dirty.put(pageId, false); // Non modifiée au départ
    }

    /**
     * Trouve une page à évincer selon la politique courante (LRU ou MRU).
     * @return Identifiant de la page à évincer
     */
    private PageId findPageToEvict() {
        if (dbConfig.getBm_policy() == DBConfig.BMpolicy.LRU) {
            return findLeastRecentlyUsedPage(); // LRU : page la moins récemment utilisée
        } else { // MRU
            return findMostRecentlyUsedPage(); // MRU : page la plus récemment utilisée
        }
    }

    /**
     * Trouve la page la plus récemment utilisée (MRU).
     * @return Identifiant de la page MRU
     */
    private PageId findMostRecentlyUsedPage() {
        for (PageId pageId : bufferPool.keySet()) {
            return pageId; // Retourne la dernière page rencontrée
        }
        return null; // Aucun élément trouvé
    }

    /**
     * Trouve la page la moins récemment utilisée (LRU).
     * @return Identifiant de la page LRU
     */
    private PageId findLeastRecentlyUsedPage() {
        for (PageId pageId : bufferPool.keySet()) {
            return pageId; // Retourne la première page rencontrée
        }
        return null; // Aucun élément trouvé
    }

    /**
     * Libère une page en décrémentant son compteur de pins.
     * Met à jour le flag "dirty" si nécessaire.
     * @param pageId Identifiant de la page
     * @param isDirty Indique si la page a été modifiée
     */
    public void freePage(PageId pageId, boolean isDirty) {
        if (pin_count.containsKey(pageId)) {
            int currentPinCount = pin_count.get(pageId);
            if (currentPinCount > 0) {
                pin_count.put(pageId, currentPinCount - 1);
                flag_dirty.put(pageId, isDirty || flag_dirty.getOrDefault(pageId, false));
            }
        }
    }

    /**
     * Vide tous les buffers en écrivant les pages modifiées sur disque.
     * @throws IOException En cas d'erreur d'écriture
     */
    public void flushBuffers() throws IOException {
        for (Map.Entry<PageId, ByteBuffer> entry : bufferPool.entrySet()) {
            PageId pageId = entry.getKey();
            ByteBuffer buffer = entry.getValue();

            if (Boolean.TRUE.equals(flag_dirty.get(pageId))) {
                getDiskManager().writePage(pageId, buffer);
            }
        }

        bufferPool.clear();
        flag_dirty.clear();
        pin_count.clear();
    }


    /**
     * Vérifie si une page donnée est marquée comme "dirty".
     * @param pageId Identifiant de la page
     * @return True si la page est modifiée, False sinon
     * @throws IOException Si la page n'est pas présente dans le buffer
     */
    public Boolean getFlagDirty(PageId pageId) throws IOException {
        Boolean isDirty = flag_dirty.get(pageId);
        if (isDirty == null) {
            throw new IOException("La pageId spécifiée n'existe pas dans le buffer pool.");
        }
        return isDirty;
    }

    /**
     * Vérifie si une page est chargée dans le buffer pool.
     * @param pageId Identifiant de la page
     * @return True si la page est chargée, False sinon
     */
    public Boolean isLoad(PageId pageId) {
        return bufferPool.containsKey(pageId);
    }

	public DiskManager getDiskManager() {
		return diskManager;
	}

    public Map<PageId, Boolean> getFlagDirtyMap() {
        return flag_dirty;
    }

    public Map<PageId, ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    public Map<PageId, Integer> getPinCount() {
        return pin_count;
    }
}
