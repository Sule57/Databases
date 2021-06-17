//// CTS2: Susic, Marin (3138441)

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;

public class MapReport {

	private static Connection c = null;
	private static PreparedStatement[] mapinfo = new PreparedStatement[7];
	private static ResultSet mapresult = null;
	private static ResultSet mapresult1 = null;
	
	private static final String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String connectionDescriptor =
			"jdbc:sqlserver://i-mssql-01.informatik.hs-ulm.de;databasename=kratzer_db";
	
	private static void makeAllStatements() {
		try {
			mapinfo[1] = c.prepareStatement(
					"SELECT M.Name " +
					"FROM [HS-ULM\\KRATZER].MAP M " +
					"WHERE M.ID = ?"
				);
			mapinfo[2] = c.prepareStatement (
					"SELECT COUNT (*) " +
					"FROM [HS-ULM\\KRATZER].CITY " +
					"WHERE MapID = ?"
					);
			mapinfo[3] = c.prepareStatement(
					"SELECT COUNT (*) " +
					"FROM [HS-ULM\\KRATZER].ROAD " +
					"WHERE MapID = ?"
					);
			mapinfo[4] = c.prepareStatement(
					"SELECT AVG(Distance) " +
					"FROM [HS-ULM\\KRATZER].ROAD " +
					"WHERE MapID = ?"
					);
			mapinfo[5] = c.prepareStatement(
					"SELECT MAX(Distance) " +
					"FROM [HS-ULM\\KRATZER].ROAD " +
					"WHERE MapID = ?"
					);
			mapinfo[6] = c.prepareStatement(
					"SELECT Name " +
					"FROM [HS-ULM\\KRATZER].CITY JOIN [HS-ULM\\KRATZER].ROAD ON [HS-ULM\\KRATZER].CITY.ID = [HS-ULM\\KRATZER].ROAD.IDfrom " +
					"WHERE Distance = ? " +
					"AND [HS-ULM\\KRATZER].ROAD.MapID = ?"
					);
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String getCity2 (int maxDistance, int id) {
		String city2 = "";
		try {
			mapinfo[6].setInt(1, maxDistance);
			mapinfo[6].setInt(2, id);
			mapresult = mapinfo[6].executeQuery();
			while(mapresult.next()) {
				city2 = mapresult.getString(1);
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return city2;
	}
	
	
	private static String getCity1 (int maxDistance, int id) {
		String city1 = "";
		try {
			mapinfo[6].setInt(1, maxDistance);
			mapinfo[6].setInt(2, id);
			mapresult = mapinfo[6].executeQuery();
			int i = 0;
			while(mapresult.next()) {
				if (i == 0) {
				city1 = mapresult.getString(1); }
				i++;
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return city1;
	}
	
	private static int getMaxDistance(int id) {
		int distance = 0;
		try {
			mapinfo[5].setInt(1, id);
			mapresult = mapinfo[5].executeQuery();
			while (mapresult.next()) {
				distance = mapresult.getInt(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return distance;
	}
	
	
	
	private static int getAverageLength(int id) {
		int length = 0;
		try {
			mapinfo[4].setInt(1, id);
			mapresult = mapinfo[4].executeQuery();
			while (mapresult.next()) {
				length = mapresult.getInt(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return length;
	}
	
	private static int getNumOfRoads(int id) {
		int roads = 0;
		try {
			mapinfo[3].setInt(1, id);
			mapresult = mapinfo[3].executeQuery();
			while (mapresult.next()) {
				roads = mapresult.getInt(1);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return roads;
	}
	
	
	
	private static String getMapName (int id) {
		String name = "";
		try {
			mapinfo[1].setInt(1, id);
			mapresult = mapinfo[1].executeQuery();
			while(mapresult.next()) {
			name = mapresult.getString(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return name;
	}
	
	private static int getNumOfCities (int id) {
		int cities = 0;
		try {
			mapinfo[2].setInt(1, id);
			mapresult = mapinfo[2].executeQuery();
			while (mapresult.next()) {
				cities = mapresult.getInt(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cities;
	}
	
	
	
	public static void main(String[] args) {
		
		try {
			// Establish JDBC connection
			
			Class.forName(driverClass);
			c =  DriverManager.getConnection(connectionDescriptor, "reader_kratzer_db", "thu123456!");
			
			// Create the prepared statements
			makeAllStatements();
			
			mapinfo[0] = c.prepareStatement(
							"SELECT M.ID " +
							"FROM [HS-ULM\\KRATZER].MAP M " +
							"ORDER BY M.ID ASC"
					);
			
			
			// Execute mapinfo and loop through the result

			mapresult1 = mapinfo[0].executeQuery();

			while (mapresult1.next()) {
				int id = mapresult1.getInt(1);
				// write output
				Terminal.put(
					"--------------------------------------- \n" +
					"Map " + getMapName(id) + " (" + id + "): \n" +
					"Cities: " + getNumOfCities(id) + "\n" +
					"Roads: " + getNumOfRoads(id)/2 + "\n" +
					"Average Road Length: " + getAverageLength(id) + " km \n" +
					"The longest road runs from " + getCity1(getMaxDistance(id),id) + " to " + getCity2(getMaxDistance(id),id) + " length: " + getMaxDistance(id) + " km" +
					"\n--------------------------------------- "
				);
			}
			
			mapresult.close();
			
		} catch (ClassNotFoundException e) {
			Terminal.put("Unable to open driver class ...");
		} catch (SQLException e) {
			Terminal.put("Database error: " + e.getMessage());
		} finally {
			// Close everything down
			try {
				if (c != null) c.close();
			} catch (SQLException e) {}
		}
	}

}
