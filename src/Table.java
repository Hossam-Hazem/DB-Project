import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class Table implements Serializable {
	private ArrayList<String> allPages;
	private int nameCounter;
	private String pagesDirectory;
	private String tableName;
	

	private static final long serialVersionUID = 1928828356132285922L;

	public Table(String name) throws IOException {
		this.allPages = new ArrayList<String>();
		this.tableName = name;
		this.nameCounter = 0;
		pagesDirectory = "data/pages/"+tableName;
		createFolder(pagesDirectory);
	}
	
	public void createFolder(String x){
		File saveDir = new File(x);
		if(!saveDir.exists()){
		    saveDir.mkdirs(); 
		}
	}
/*
	public void createPage() throws IOException {
		Page x = new Page(tableName, "" + pagesNames.size());
		File saveDir = new File("data/" + tableName);
		if (!saveDir.exists()) {
			saveDir.mkdirs();
		}
		FileOutputStream fs = new FileOutputStream("data/" + tableName + "/"
				+ pagesNames.size() + ".bin");
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(x);
		os.close();
		fs.close();
		// pagesNames.add(x.);
	}

*/
		
	public  void createPage() throws IOException{
		Page x = new Page(""+nameCounter);
		String path = pagesDirectory+"/"+x.getPageName()+".bin";
		FileOutputStream fs = new FileOutputStream(path);
		ObjectOutputStream os = new ObjectOutputStream(fs);
		os.writeObject(x);
		os.close();
		fs.close();
		allPages.add(""+nameCounter);
		nameCounter++;
		
	}
	
	

	public ArrayList<String> getAllPages() {
		return allPages;
	}

	

	public int getNameCounter() {
		return nameCounter;
	}

	public void setNameCounter(int nameCounter) {
		this.nameCounter = nameCounter;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	
	public String getPagesDirectory() {
		return pagesDirectory;
	}

	public void setPagesDirectory(String pagesDirectory) {
		this.pagesDirectory = pagesDirectory;
	}

	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void addPagetoArray(Page x){
		allPages.add(x.getPageName());
	}
	
	


	public static void main(String[] args) throws IOException {
		new Table("Test2");
	}
}