import java.io.IOException;
import java.util.Hashtable;


public class DBAppTest {
	static DBApp testingAll;
	
	public static void main(String[] args) throws DBAppException, IOException, DBEngineException, ClassNotFoundException {
		testingAll = new DBApp();
		
		// createTable Test
		/*
		testingAll.init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("age", "java.lang.Integer");
		htblColNameType.put("ID", "java.lang.Integer");
		htblColNameType.put("major", "java.lang.String");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
		htblColNameRefs.put("ID", "table1.id");

		// ===> execute once and comment createTable and execute multiple
		testingAll.createTable("testCreateTable", htblColNameType, htblColNameRefs,
				"ID");
		testingAll.saveAll();
		*/
		
		//----------------------------------------------------------------------------------------
		
		// createIndex Test
		/*
		testingAll.init();
		testingAll.createIndex("testCreateTable", "name");
		testingAll.saveAll();
		*/
		
		//----------------------------------------------------------------------------------------
		
		// insertIntoTable Test
		
		// Wrong Insertion... Invalid Input Format... NO Effect
		/*
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "omar");
		insertion.put("age", "2");
		//HERE
		insertion.put("ID", "ERROR");
		insertion.put("major", "cs");
		testingAll.insertIntoTable("testCreateTable", insertion);
		testingAll.saveAll();
		*/
		
		// Wrong Insertion... Column does NOT Exist... NO Effect
		/*
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "omar");
		insertion.put("age", "2");
		// HERE
		insertion.put("ERROR", "");
		insertion.put("major", "cs");
		testingAll.insertIntoTable("testCreateTable", insertion);
		testingAll.saveAll();
		*/
		
		// Right Insertion... Page created... Record Added to page, table, tree
		/*
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "student");
		insertion.put("age", "20");
		insertion.put("major", "cs");
		insertion.put("ID", "10101010");
		testingAll.insertIntoTable("testCreateTable", insertion);
		testingAll.saveAll();
		*/
		
	}
}
