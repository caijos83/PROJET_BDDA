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
