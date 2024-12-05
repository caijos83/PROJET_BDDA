
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation {
	private String name;
	private List<String> columnNames;
	private List<String> columnTypes; // "INT", "REAL", "CHAR(T)", "VARCHAR(T)"
	private PageId headerPageId;
	private DiskManager diskManager;
	private BufferManager bufferManager;
	private List<Record> records;


    public Relation(String name) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Relation name cannot be null or empty.");
		}
		this.name = name;
		this.columnNames = new ArrayList<>();
		this.columnTypes = new ArrayList<>();
		this.records = new ArrayList<>();
	}

	public Relation(String name, PageId headerPageId, DiskManager dm, BufferManager bm) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Relation name cannot be null or empty.");
		}
		this.name = name;
		this.columnNames = new ArrayList<>();
		this.columnTypes = new ArrayList<>();
		this.headerPageId = headerPageId;
		this.diskManager = dm;
		this.bufferManager = bm;
	}

	public void addColumn(String name, String type) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Column name cannot be null or empty.");
		}
		if (type == null || type.trim().isEmpty()) {
			throw new IllegalArgumentException("Column type cannot be null or empty.");
		}
		if (!type.matches("INT|REAL|CHAR\\(\\d+\\)|VARCHAR\\(\\d+\\)")) {
			throw new IllegalArgumentException("Invalid column type: " + type);
		}
		columnNames.add(name);
		columnTypes.add(type);
	}

	public int writeRecordToBuffer(Record record, ByteBuffer buff, int pos) {
		if (record.size() != columnTypes.size()) {
			throw new IllegalArgumentException("Record size does not match the number of columns.");
		}
		if (pos < 0 || pos >= buff.capacity()) {
			throw new IllegalArgumentException("Invalid buffer position: " + pos);
		}
		buff.position(pos);
		int totalSize = 0;

		try {
			for (int i = 0; i < record.size(); i++) {
				Object value = record.getValue(i);
				String type = columnTypes.get(i);
				if (type.equals("INT")) {
					if (!(value instanceof Integer)) {
						throw new IllegalArgumentException(
								"Expected INT but found " + value.getClass().getSimpleName());
					}
					buff.putInt((Integer) value);
					totalSize += Integer.BYTES;
				} else if (type.equals("REAL")) {
					if (!(value instanceof Float)) {
						throw new IllegalArgumentException(
								"Expected REAL but found " + value.getClass().getSimpleName());
					}
					buff.putFloat((Float) value);
					totalSize += Float.BYTES;
				} else if (type.startsWith("CHAR")) {
					int length = Integer.parseInt(type.substring(5, type.length() - 1)); // Extract T
					if (!(value instanceof String)) {
						throw new IllegalArgumentException(
								"Expected CHAR but found " + value.getClass().getSimpleName());
					}
					String strValue = (String) value;
					if (strValue.length() > length) {
						throw new IllegalArgumentException("CHAR value exceeds specified length: " + length);
					}
					for (char c : strValue.toCharArray()) {
						buff.put((byte) c);
					}
					for (int j = strValue.length(); j < length; j++) {
						buff.put((byte) 0); // Null character padding
					}
					totalSize += length;
				} else if (type.startsWith("VARCHAR")) {
					if (!(value instanceof String)) {
						throw new IllegalArgumentException(
								"Expected VARCHAR but found " + value.getClass().getSimpleName());
					}
					String strValue = (String) value;
					buff.putInt(strValue.length()); // Length of the string
					for (char c : strValue.toCharArray()) {
						buff.put((byte) c);
					}
					totalSize += 4 + strValue.length();
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error writing record to buffer: " + e.getMessage(), e);
		}

		return totalSize;
	}

	public int readFromBuffer(Record record, ByteBuffer buff, int pos) {
		if (record == null) {
			throw new IllegalArgumentException("Record cannot be null.");
		}
		if (pos < 0 || pos >= buff.capacity()) {
			throw new IllegalArgumentException("Invalid buffer position: " + pos);
		}
		buff.position(pos);
		int totalSize = 0;

		try {
			for (String type : columnTypes) {
				if (type.equals("INT")) {
					record.addValue(buff.getInt());
					totalSize += Integer.BYTES;
				} else if (type.equals("REAL")) {
					record.addValue(buff.getFloat());
					totalSize += Float.BYTES;
				} else if (type.startsWith("CHAR")) {
					int length = Integer.parseInt(type.substring(5, type.length() - 1));
					byte[] charBytes = new byte[length];
					buff.get(charBytes);
					String strValue = new String(charBytes).replaceAll("\u0000+$", "");
					record.addValue(strValue);
					totalSize += length;
				} else if (type.startsWith("VARCHAR")) {
					int strLength = buff.getInt();
					byte[] strBytes = new byte[strLength];
					buff.get(strBytes);
					String strValue = new String(strBytes);
					record.addValue(strValue);
					totalSize += 4 + strLength;
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error reading record from buffer: " + e.getMessage(), e);
		}

		return totalSize;
	}


    public void saveHeaderPageId(ObjectOutputStream oos) throws IOException {
        if (headerPageId != null) {
            oos.writeInt(headerPageId.getFileIdx());
            oos.writeInt(headerPageId.getPageIdx());
        } else {
            oos.writeInt(-1); // Special marker indicating no header page
        }
    }

    public void loadHeaderPageId(ObjectInputStream ois) throws IOException {
        int fileIdx = ois.readInt();
        if (fileIdx != -1) {
            int pageIdx = ois.readInt();
            headerPageId = new PageId(fileIdx, pageIdx);
        }
    }

    public int getColumnNumber() {
        return columnNames.size();
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setHeaderPageId(PageId headerPageId) {
        this.headerPageId = headerPageId;
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public void addRecord(Record record) {
        records.add(record);
    }
	public void addDataPage() throws IOException {
		// On alloue une nouvelle page via le DiskManager
		PageId newPageId = diskManager.allocPage(tableName);

		// On charge la Header Page dans un buffer
		ByteBuffer headerBuffer = bufferManager.getPage(headerPageId);

		// Nombre de pages existantes (N) stocké au début de la Header Page
		int numPages = headerBuffer.getInt(0);

		// On calcule l'offset pour la nouvelle entrée dans le Page Directory
		int offset = 4 + numPages * 12; // Chaque entrée fait 12 octets : 8 pour le PageId, 4 pour l'espace libre

		// On ajoute les informations de la nouvelle page dans le Page Directory
		headerBuffer.putInt(offset, newPageId.getFileIdx()); // Stocker l'indice du fichier
		headerBuffer.putInt(offset + 4, newPageId.getPageIdx()); // Stocker l'indice de la page dans le fichier
		headerBuffer.putInt(offset + 8, diskManager.getConf().getPagesize()); // Taille initiale de l'espace libre (taille totale de la page)

		// Le compteur de pages (N) augmente
		headerBuffer.putInt(0, numPages + 1);

		// On marque la Header Page comme dirty
		bufferManager.freePage(headerPageId, true);

		System.out.println("Nouvelle page ajoutée : " + newPageId);
	}

	public PageId getFreeDataPageId(int sizeRecord) throws IOException {
		//Charger la Header Page dans un buffer
		ByteBuffer headerBuffer = bufferManager.getPage(headerPageId);

		//Nombre de Data Pages (N) dans la Header Page
		int numPages = headerBuffer.getInt(0); // N est stocké au début de la Header Page

		// On parcourt chaque entrée du Page Directory
		for (int i = 0; i < numPages; i++) {
			int offset = 4 + i * 12; // Calcul de l'offset de l'entrée dans le Page Directory

			// Lire les informations de la page (PageId + espace libre)
			int fileIdx = headerBuffer.getInt(offset);        // fileIdx du PageId
			int pageIdx = headerBuffer.getInt(offset + 4);    // pageIdx du PageId
			int freeSpace = headerBuffer.getInt(offset + 8);  // espace libre

			// On vérifie si cette page a assez d'espace libre pour sizeRecord
			if (freeSpace >= sizeRecord) {
				// Si oui, retourner le PageId correspondant
				bufferManager.freePage(headerPageId, false); // Libérer le buffer (pas de modification = dirty = false)
				return new PageId(fileIdx, pageIdx);
			}
		}

		// Si aucune page avec suffisamment d'espace n'est trouvée, retourner null
		bufferManager.freePage(headerPageId, false); // Libérer le buffer
		return null;
	}


	public BufferManager getBufferManager() {
		return bufferManager;
	}

	public RecordID writeRecordToDataPage(Record record, PageId pageId) throws IOException {
		// Charger la page de données dans un buffer
		ByteBuffer dataBuffer = bufferManager.getPage(pageId);
	
		// Lire la position de l'espace libre
		int freeSpaceOffset = dataBuffer.getInt(dataBuffer.capacity() - 8);
	
		// Lire le nombre de slots
		int slotCount = dataBuffer.getInt(dataBuffer.capacity() - 4);
	
		// Positionner le buffer à l'espace libre
		dataBuffer.position(freeSpaceOffset);
	
		// Écrire le record dans le buffer
		int recordSize = writeRecordToBuffer(record, dataBuffer, freeSpaceOffset);
	
		// Mettre à jour le Slot Directory
		int slotPosition = dataBuffer.capacity() - 8 - (slotCount + 1) * 8;
		dataBuffer.putInt(slotPosition, freeSpaceOffset); // Début du record
		dataBuffer.putInt(slotPosition + 4, recordSize); // Taille du record
	
		// Mettre à jour l'espace libre
		freeSpaceOffset += recordSize;
		dataBuffer.putInt(dataBuffer.capacity() - 8, freeSpaceOffset);
	
		// Incrémenter le nombre de slots
		dataBuffer.putInt(dataBuffer.capacity() - 4, slotCount + 1);
	
		// Marquer la page comme modifiée
		bufferManager.freePage(pageId, true);
	
		// Retourner le RecordId correspondant
		return new RecordID(pageId, slotCount); // slotCount est l'index du nouveau record
	}
	
	public List<Record> getRecordsInDataPage(PageId pageId) throws IOException {
		List<Record> records = new ArrayList<>();
	
		// Charger la page de données dans un buffer
		ByteBuffer dataBuffer = bufferManager.getPage(pageId);
	
		// Lire le nombre de slots
		int slotCount = dataBuffer.getInt(dataBuffer.capacity() - 4);
	
		// Parcourir le Slot Directory
		for (int i = 0; i < slotCount; i++) {
			int slotPosition = dataBuffer.capacity() - 8 - (i + 1) * 8;
			int recordOffset = dataBuffer.getInt(slotPosition);
			int recordSize = dataBuffer.getInt(slotPosition + 4);
	
			// Si le record n'a pas été supprimé (taille > 0)
			if (recordSize > 0) {
				// Lire le record depuis le buffer
				Record record = new Record();
				readFromBuffer(record, dataBuffer, recordOffset);
				records.add(record);
			}
		}
	
		// Libérer la page après lecture
		bufferManager.freePage(pageId, false);
	
		return records;
	}

	public List<PageId> getDataPages() throws IOException {
		List<PageId> pages = new ArrayList<>();

		// Charger la Header Page dans un buffer
		ByteBuffer headerBuffer = bufferManager.getPage(headerPageId);

		try {
			// Lire le nombre total de Data Pages (N)
			int numPages = headerBuffer.getInt(0);

			// Parcourir chaque entrée du Page Directory
			for (int i = 0; i < numPages; i++) {
				int offset = 4 + i * 12; // Calcul de l'offset de l'entrée dans le Page Directory

				// Lire les informations de la page (PageId)
				int fileIdx = headerBuffer.getInt(offset);        // fileIdx du PageId
				int pageIdx = headerBuffer.getInt(offset + 4);    // pageIdx du PageId
				PageId pageId = new PageId(fileIdx, pageIdx);

				// Ajouter le PageId à la liste
				pages.add(pageId);
			}
		} finally {
			// Libérer la Header Page après lecture
			bufferManager.freePage(headerPageId, false);
		}

		return pages;
	}

	public RecordID insertRecord(Record record) throws IOException {
		// Trouver une page ayant suffisamment d'espace libre
		int recordSize = record.toString().getBytes().length; // Approximation de la taille du record
		PageId freePageId = getFreeDataPageId(recordSize);

		//Si aucune page n'a assez d'espace, allouer une nouvelle page
		if (freePageId == null) {
			addDataPage(); // Ajout d'une nouvelle page
			freePageId = getFreeDataPageId(recordSize);
		}

		//Charger la page dans un buffer
		ByteBuffer pageBuffer = bufferManager.getPage(freePageId);

		try {
			// Insérer le record dans la page
			int slotIdx = insertIntoPage(pageBuffer, record);

			// Mettre à jour l'espace libre dans le Page Directory
			updateFreeSpace(freePageId, -recordSize);

			// 6. Retourner le RecordId correspondant
			return new RecordID(freePageId, slotIdx);
		} finally {
			// Libérer la page après l'insertion
			bufferManager.freePage(freePageId, true);
		}
	}

	private int insertIntoPage(ByteBuffer pageBuffer, Record record) {
		// Récupérer l'emplacement de l'espace libre
		int freeSpaceOffset = pageBuffer.getInt(pageBuffer.capacity() - 4);

		// Convertir le Record en bytes
		byte[] recordBytes = record.toString().getBytes();

		// Écrire les données du Record dans la page
		pageBuffer.position(freeSpaceOffset);
		pageBuffer.put(recordBytes);

		// Mettre à jour le Slot Directory
		int slotIdx = pageBuffer.getInt(pageBuffer.capacity() - 8); // Nombre de slots
		int slotOffset = pageBuffer.capacity() - 8 - (slotIdx + 1) * 8;

		pageBuffer.putInt(slotOffset, freeSpaceOffset); // Position
		pageBuffer.putInt(slotOffset + 4, recordBytes.length); // Taille

		// Mettre à jour le compteur de slots et l'emplacement de l'espace libre
		pageBuffer.putInt(pageBuffer.capacity() - 8, slotIdx + 1); // Nombre de slots
		pageBuffer.putInt(pageBuffer.capacity() - 4, freeSpaceOffset + recordBytes.length); // Nouvel offset

		return slotIdx;
	}

	private void updateFreeSpace(PageId pageId, int sizeChange) throws IOException {
		ByteBuffer headerBuffer = bufferManager.getPage(headerPageId);

		try {
			// Parcourir le Page Directory pour trouver la page
			int numPages = headerBuffer.getInt(0);
			for (int i = 0; i < numPages; i++) {
				int offset = 4 + i * 12;
				int fileIdx = headerBuffer.getInt(offset);
				int pageIdx = headerBuffer.getInt(offset + 4);

				if (fileIdx == pageId.getFileIdx() && pageIdx == pageId.getPageIdx()) {
					// Mettre à jour l'espace libre
					int currentFreeSpace = headerBuffer.getInt(offset + 8);
					headerBuffer.putInt(offset + 8, currentFreeSpace + sizeChange);
					break;
				}
			}
		} finally {
			// Libérer la Header Page après modifications
			bufferManager.freePage(headerPageId, true);
		}
	}


	
	public List<Record> GetAllRecords() throws IOException {
        List<Record> allRecords = new ArrayList<>(); // Liste pour stocker tous les records

        // Récupérer la liste des PageId des pages de données
        List<PageId> dataPages = getDataPages();

        // Parcourir chaque page de données et extraire les records
        for (PageId pageId : dataPages) {
            // Récupérer les records d'une page spécifique
            List<Record> recordsInPage = getRecordsInDataPage(pageId);

            // Ajouter ces records à la liste principale
            allRecords.addAll(recordsInPage);
        }

        return allRecords;
    }

}
