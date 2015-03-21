import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements RequiredMethods {
	public static void main(String[] args) {
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("col1", "str");
		htblColNameType.put("col2", "int");
		htblColNameType.put("col3", "int");
		htblColNameType.put("col4", "str");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
		htblColNameRefs.put("col1", "table1.id");

		DBApp app = new DBApp();

		try {
			app.createTable("kareem", htblColNameType, htblColNameRefs, "col1");
		} catch (DBAppException e) {
			e.printStackTrace();
		}

		try {
			app.createIndex("kareem", "col2");
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException {
		// TODO Auto-generated method stub

		String metaInfo = "";
		Enumeration ColNames = htblColNameType.keys();
		while (ColNames.hasMoreElements()) {
			String ColName = (String) ColNames.nextElement();
			metaInfo += strTableName + ", " + ColName + ", "
					+ htblColNameType.get(ColName) + ", "
					+ ((ColName.equals(strKeyColName)) ? "true" : "false")
					+ ", " + "false" + ", " + htblColNameRefs.get(ColName)
					+ "\n";
		}
		try {
			writeMetaAppend("metadata.csv", metaInfo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void writeMetaAppend(String fileName, String text)
			throws IOException {
		File f = new File(fileName);
		if (!f.exists()) {
			text = "Table Name, Column Name, Column Type, Key, Indexed, References"
					+ "\n" + text;
		}
		String tmp = readFile(fileName) + text;
		BufferedWriter output = new BufferedWriter(new FileWriter(fileName));
		output.write(tmp);
		output.flush();
		output.close();
	}

	public void writeFile(String fileName, String text) throws IOException {

		BufferedWriter output = new BufferedWriter(new FileWriter(fileName));
		output.write(text);
		output.close();
	}

	public String readFile(String fileName) throws IOException {
		String ret = "";
		File f = new File(fileName);
		if (f.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String line = "";
			while ((line = br.readLine()) != null) {
				ret += line + "\n";
			}
			br.close();
		}
		return ret;
	}

	@Override
	public void createIndex(String strTableName, String strColName)
			throws DBAppException {
		// TODO Auto-generated method stub

		// things to do

		StringBuilder meta = new StringBuilder();
		try {
			meta.append(readFile("metadata.csv"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int i = 0;
		i = meta.indexOf(strTableName + ", " + strColName);
		if (i != -1) {
			int semCount = 0;
			while (i < meta.length()) {
				if (semCount == 4) {
					if(meta.substring(i + 1, i + 6).equals("false"))
					meta.replace(i + 1, i + 6, "true");
					break;
				}
				if (meta.charAt(i) == ',') {
					semCount++;
				}
				i++;
			}
			try {
				writeFile("metadata.csv", meta.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("table is not found !");
		}
	}

	@Override
	public void insertIntoTable(String strTableName,
			Hashtable<String, String> htblColNameValue) throws DBAppException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator selectValueFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator selectRangeFromTable(String strTable,
			Hashtable<String, String> htblColNameRange, String strOperator)
			throws DBEngineException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveAll() throws DBEngineException {
		// TODO Auto-generated method stub

	}

}
