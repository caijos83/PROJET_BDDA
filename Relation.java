package miniSGBDR;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation {
	private String name;
	private List<String> columnNames;
	private List<String> columnTypes; // "INT", "REAL", "CHAR(T)", "VARCHAR(T)"

	public Relation(String name) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Relation name cannot be null or empty.");
		}
		this.name = name;
		this.columnNames = new ArrayList<>();
		this.columnTypes = new ArrayList<>();
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

	public int getColumnNumber() {
		return columnNames.size();
	}

	public String getName() {
		return name;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}
}
