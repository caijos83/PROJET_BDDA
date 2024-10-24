public void writePage(PageId pageId, ByteBuffer buff) throws IOException {
        // Vérification de la capacité du ByteBuffer
        if (buff.remaining() < config.getPagesize()) {
            throw new BufferOverflowException(); // Lever l'exception si le buffer est plein
        }

        try (RandomAccessFile file = new RandomAccessFile(config.getDbpath() + "/F" + pageId.getFileIdx() + ".rsdb", "rw")) {
            file.seek((long) pageId.getPageIdx() * config.getPagesize());
            byte[] bytes = new byte[config.getPagesize()];
            buff.get(bytes); // Lire les données du ByteBuffer

            // Écrire les octets dans le fichier
            file.write(bytes);
        } catch (IOException e) {
            throw new IOException("Erreur lors de l'écriture de la page " + pageId, e);
        }
    }
