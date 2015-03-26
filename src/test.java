import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


public class test {
	public static void main(String[] args) throws NoSuchMethodException, SecurityException, ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String strColType = "java.lang.Integer";
		 String strColValue = "100";
		 Class x = Class.forName( strColType );
		// System.out.println(x);
		 //Constructor constructor = x.;
		 Object y =  x.getDeclaredConstructor(String.class).newInstance(strColValue);
		 System.out.println(y.toString());
	}
}
