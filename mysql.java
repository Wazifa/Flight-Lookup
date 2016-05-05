// Name:	 Wazifa Jafar
// Description: connecting mysql through JDBC
import java.sql.*;
import java.util.Scanner;
import java.util.Date;
import java.text.DateFormat;
import java.util.*;

final class mysql {
    final static String user = "XXX"; //replace XXX with your NetID
    final static String password = "YYY"; //replace YYY with your mysql password
    final static String db = "DB"; //replace DB with your NetID -> your database 
    final static String jdbc = "jdbc:mysql://localhost:3306/"+db+"?user="+user+"&password="+password;
    
    public static void main ( String[] args ) throws Exception 
    {
        
	int loop =1;
	while(loop == 1)
	{
		System.out.println("Main Menu: ");
	        System.out.println("1. Press 1 for Departure.");
                System.out.println("2. Press 2 for Arrival.");
                System.out.println("0. Press 0 to exit the program.");
		
		int FLNO, Seq, menu, found = 0, pilot;
		String date, dept_time, arr_time;
	
		Scanner input = new Scanner(System.in);
	        Class.forName("com.mysql.jdbc.Driver").newInstance();
	        Connection con = DriverManager.getConnection(jdbc);
	        Statement stmt = con.createStatement();
        
		menu = input.nextInt();

		if (menu == 0)
		{
			loop = -1;
			System.out.println("Exiting the program.");
			break;
		}
		System.out.println("Enter the flight number: ");
		FLNO = input.nextInt();
		System.out.println("Enter the seq number: ");
		Seq = input.nextInt();
		System.out.println("Enter the date: ");
		date = input.next();
		java.sql.Date d1=java.sql.Date.valueOf(date); 
		
		if (Seq > 2 || Seq <= 0)
		{
			System.out.println("No such leg of the flight instance exists");
			continue;
		}
		
		if (menu == 1)
		{
			ResultSet rs = stmt.executeQuery("select * from FlightLeg");	
        		while (rs.next())
			{
				// check for correct seq number
				if (rs.getInt ("FLNO") == FLNO && rs.getInt("Seq") == 2 && Seq == 2)
				{
					ResultSet rss = stmt.executeQuery("select * from FlightLegInstance");
                                        int existing_flight = 0, existing_pilot =0;

					System.out.println("Please enter the departure time.");
                                        dept_time = input.next();
                                        System.out.println("Please enter the id of the pilot: ");
                                        pilot = input.nextInt();
                                        
                                        // checking if the flight leg instance already exists
                                        while (rss.next())
                                        {
                                                if((rss.getInt("FLNO")== FLNO) && (rss.getInt("Seq") == 2) && rss.getString("FDate").equals(date))
                                                {
                                                        System.out.println("The flight already exists.");
                                                        existing_flight = 1;
                                                        found = 1;
                                                        break;
                                                }
                                        }
					
					// check if pilot already exists
					ResultSet ch_pilot = stmt.executeQuery("Select * from Pilot");
					while(ch_pilot.next())
					{
						if (ch_pilot.getInt("ID")== pilot)
						{
							existing_pilot = 1;
							break;
						}
					}
                                        if (existing_flight == 1)
                                                break;
					else if (existing_pilot == 0)
					{
						System.out.println("There is no such pilot available.");
						found = 1;
						break;
					}

					//  check if date exists in flight instance table; if not, will add it.
					ResultSet rs2 = stmt.executeQuery("select * from FlightInstance");
                                        int existing_flight_instance =  0;

                                        while(rs2.next())
                                        {
                                                if (rs2.getInt("FLNO") == FLNO && rs2.getString("FDate").equals(date))
                                                {
                                                        existing_flight_instance = 1;
                                                        break;
                                                }
                                        }


                                        if (existing_flight_instance == 0)
                                        {
                         	               String insert_FI = "INSERT INTO FlightInstance " + "VALUES (?, ?)";
                                               PreparedStatement ps_FI = con.prepareStatement (insert_FI);
                                               ps_FI.setInt(1, FLNO);
                                               ps_FI.setDate(2, d1);
                                               ps_FI.execute();
                                        }

					PreparedStatement rsq = con.prepareStatement ("select * from FlightLegInstance where ActArr < STR_TO_DATE(?, '%H:%i:%s');");
						
					rsq.setString(1, dept_time);
					ResultSet rs1 = rsq.executeQuery();

					int existing_FLI = 0;
														
					while (rs1.next())
					{
						if (rs1.getInt("FLNO") == FLNO && rs1.getInt("Seq")== 1 && rs1.getString("ActArr") != null && rs1.getString("FDate").equals(date))
						{
							existing_FLI = 1;
							break;
						}
					}
							
					if (existing_FLI == 0)
					{
						System.out.println("Could not add the flight leg instance.");
						found = 1;
						break;
					}
					else
					{
						String insert_FLI = "INSERT INTO FlightLegInstance (FLNO, Seq, FDate, ActDept, Pilot)" + " VALUES(?, ?, ?, ?, ?)";
	                                        PreparedStatement ps_FLI = con.prepareStatement(insert_FLI);
        	                                ps_FLI.setInt(1, FLNO);
                	                        ps_FLI.setInt(2, Seq);
                                                ps_FLI.setDate(3, d1);
                                                ps_FLI.setString(4, dept_time);
						ps_FLI.setInt(5, pilot);
                                                ps_FLI.execute();
						found = 1;
                                                System.out.println("The flight has been added successfully.");
                                                break;
					}	
				}
				else  if (rs.getInt("FLNO")== FLNO && rs.getInt("Seq") == 1 && Seq == 1)
				{		
					ResultSet rs1 = stmt.executeQuery("select * from FlightLegInstance");		
					int existing_flight = 0;

					// checking if the flight leg instance already exists
					while (rs1.next())		
					{
						if((rs1.getInt("FLNO")== FLNO) && (rs1.getInt("Seq") == 1) && rs1.getString("FDate").equals(date))
						{
							System.out.println("The flight already exists.");
							existing_flight = 1;
							found = 1;
							break;
						} 						
					}	

					if (existing_flight == 1)
						break;

					String insert_FLI = "INSERT INTO FlightLegInstance (FLNO, Seq, FDate, ActDept, Pilot)" + "VALUES(?, ?, ?, ?, ?)";
					
					// Check statements to do the insert only if it does not exist in FlightInstance already
					ResultSet rs2 = stmt.executeQuery("select * from FlightInstance");
					int existing_flight_instance =  0;

					while(rs2.next())
					{
						if (rs2.getInt("FLNO") == FLNO && rs2.getString("FDate").equals(date))
						{
							existing_flight_instance = 1;
							break;							
						}
					}	

						
						if (existing_flight_instance == 0)
						{
							String insert_FI = "INSERT INTO FlightInstance " + "VALUES (?, ?)";
							PreparedStatement ps_FI = con.prepareStatement (insert_FI);
							ps_FI.setInt(1, FLNO);
							ps_FI.setDate(2, d1);
							ps_FI.execute();
						}	

						System.out.println("Please add the departure time.");
                                                dept_time = input.next();
                                                System.out.println("Please enter the id of the pilot: ");
                                                pilot = input.nextInt();
						int existing_pilot = 0;

						// check if pilot already exists
	                                        ResultSet ch_pilot = stmt.executeQuery("Select * from Pilot");
        	                                while(ch_pilot.next())
                	                        {
                        	                        if (ch_pilot.getInt("ID")== pilot)
                                	                {
                                        	                existing_pilot = 1;
								found = 1;
                                                	        break;
                                    	            }
                                        	}

						if (existing_pilot == 0)
						{
							System.out.println("The requested pilot is not avaible. Insertion unsuccessful!");
							break;
						}

						PreparedStatement ps_FLI = con.prepareStatement(insert_FLI);
						ps_FLI.setInt(1, FLNO);
						ps_FLI.setInt(2, Seq);
						ps_FLI.setDate(3, d1);
						ps_FLI.setString(4, dept_time);
						ps_FLI.setInt(5, pilot);
						ps_FLI.execute();
						found = 1;

						System.out.println("Success: The flight leg instance record has been created.");
						break;
					}
				}
				if (found == 0)
					System.out.println("There is no such flight available.");
			}
			else if (menu == 2)
			{
				System.out.println("Please enter the arrival time.");
				arr_time = input.next();

				PreparedStatement rsq = con.prepareStatement("select * from FlightLegInstance where FLNO = ? AND Seq = ? AND ActDept < STR_TO_DATE(?, '%H:%i:%s')");
                                rsq.setInt(1, FLNO);
				rsq.setInt(2, Seq);
				rsq.setString(3, arr_time);
                                ResultSet rs = rsq.executeQuery();
	
				// CHECKING FOR DURATION TO BE MORE OR EQUAL TO THE DURATION REQUIRED 
                                long duration_instance;
                                int result;
				int wrong_duration =0;

                                PreparedStatement ch_fl = con.prepareStatement ("select timediff(ArrTime, DeptTime) from FlightLeg where FLNO=? AND Seq=?");

                                PreparedStatement ch_fli = con.prepareStatement("Select ActDept from FlightLegInstance where FLNO = ? AND Seq = Seq = ? AND FDate = ?");
                                ch_fl.setInt(1, FLNO);
                                ch_fl.setInt(2, Seq);
                                ResultSet rs_fl = ch_fl.executeQuery();

                                ch_fli.setInt(1, FLNO);
                                ch_fli.setInt(2, Seq);
                                ch_fli.setString(3, date);
                                ResultSet rs_fli = ch_fli.executeQuery();

                                while (rs_fl.next())
                                {
                                        long t1=java.sql.Time.valueOf(rs_fl.getString("timediff(ArrTime, DeptTime)")).getTime();		// actual duration from FlightLeg
                                        while(rs_fli.next())
                                        {
                                                long t3=java.sql.Time.valueOf(rs_fli.getString("ActDept")).getTime();				// departure time from flight leg instance
						long t4 = java.sql.Time.valueOf(arr_time).getTime();					// arrival time given by user
						duration_instance = t4 - t3;
					
						if (duration_instance < t1-21600000)
						{
							System.out.println("The duration is not long enough.");
							wrong_duration = 1;
							break;
						}			
                                        }
                                }
	
				int existing_instance = 0;
				while(rs.next())
				{
					if (rs.getInt("FLNO") == FLNO && rs.getInt("Seq") == Seq && rs.getString("ActDept") != null && rs.getString("FDate").equals(date) && rs.getString("ActArr") == null && wrong_duration == 0)
					{
						existing_instance = 1;

						String update = "Update FlightLegInstance SET ActArr = ? where FLNO = ? AND Seq = ? AND FDate = ?";
						PreparedStatement up = con.prepareStatement(update);
						up.setString(1, arr_time);
						up.setInt(2, FLNO);
						up.setInt(3, Seq);
						up.setDate(4, d1);
						up.executeUpdate();
						System.out.println("The arrival time has been updated successfully.");
						break;
					}
				}	
				if (existing_instance == 0)
					System.out.println("Error: Arrival time could not be updated. Check if a prior flight exists or not and if it has landed.");			
				rs.close();
			}			
      		  stmt.close();
       		  con.close();
		}
	}
   }


