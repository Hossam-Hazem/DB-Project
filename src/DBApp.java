import java.io.BufferedReader;
import java.io.BufferedWriter;
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

		String metaInfo = "Table Name, Column Name, Column Type, Key, Indexed, References"
				+ "\n";

		Enumeration ColNames = htblColNameType.keys();
		while (ColNames.hasMoreElements()) {
			String ColName = (String) ColNames.nextElement();
			metaInfo += strTableName + ", " + ColName + ","
					+ htblColNameType.get(ColName) + ", "
					+ ((ColName.equals(strKeyColName)) ? "true" : "false")
					+ ", "
					+ ((ColName.equals(strKeyColName)) ? "true" : "false")
					+ ", " + htblColNameRefs.get(ColName) + "\n";
		}
		try {
			writeFile(strTableName + ".meta", metaInfo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void writeFile(String fileName, String text) throws IOException {

		BufferedWriter output = new BufferedWriter(new FileWriter(fileName));
		output.write(text);
		output.close();
	}

	public void readFile(String fileName) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		br.readLine();
		br.close();

	}

	@Override
	public void createIndex(String strTableName, String strColName)
			throws DBAppException {
		// TODO Auto-generated method stub

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
