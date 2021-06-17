

/*
 * Created on 29.10.2004
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */


/**
 * @author kratzer
 *
 * To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */

import java.io.*;

public class Terminal {
	
	public static String getString(String prompt) {
		
		if (!prompt.equals("")) put(prompt);
		
		InputStreamReader reader = new InputStreamReader(System.in);
		BufferedReader    input  = new BufferedReader(reader);
		
		try {
			return input.readLine();
		} catch (Exception e) {
			return "";
		} 
	}
	
	public static int getInt(String prompt) {
		
		int resultat = 0;
		
		try {
			resultat = Integer.parseInt(getString(prompt));
		} catch (Exception e) {
			put("Bitte eine ganze Zahl (Betrag < 2E9) eingeben!");
			resultat = getInt(prompt);
		}
		
		return resultat;
	}
	
	public static double getDouble(String prompt) {
		
		double resultat = 0.0;
		
		try {
			resultat = Double.parseDouble(getString(prompt));
		} catch (Exception e) {
			put("Bitte eine ordentliche Gleitkommazahl eingeben!");
			resultat = getDouble(prompt);
		}
		
		return resultat;
	}
	
	public static void put(String s) {
		
		System.out.println(s);
	}
	
	public static void put(int s) {
		
		System.out.println(s);
	}
	
	public static void put(double s) {
		
		System.out.println(s);
		
	}
	
	
}
