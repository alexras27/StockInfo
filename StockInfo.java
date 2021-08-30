import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class StockInfo {

    static Connection conn = null;
    static double adj;

    public static void main(String[] args) throws Exception {
        // Get connection properties
    	Deque<stockDay> deque = new LinkedList<stockDay>();
        String paramsFile = "ConnectionParameters.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }
        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));
        
        try {
            // Get connection
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n", dburl, username);

            //showCompanies();

            // Enter Ticker and TransDate, Fetch data for that ticker and date
			Scanner in = new Scanner(System.in);
            String [] range = new String[2];
            
            while (true) {
                System.out.print("Enter ticker symbol [start/end dates]");
                String[] data = in.nextLine().trim().split("\\s+");
                if (data.length > 3 || data.length==0)
                   break;
                if(printName(data[0])==1) {
	                range = getRange(data[0]);
	                if(data.length == 3) {
	                	System.out.println(range[0].compareTo(data[1])+"|"+range[1].compareTo(data[0]));
	                	if(before(range[0], data[1]) && before(data[2], range[0])) {
	                		range[0] = data[1];
	                    	range[1] = data[2];
	                    	getInfo(deque,data[0], range[0], range[1]);
	                        System.out.println();
	                        invest(deque, range[1]);
	                	}
	                	else {
	                		System.out.println("dates out of range: "+range[0]+"--"+ range[1]);
	                		break;
	                	}
	                	
	                }
	                else {
	                	
	                	getInfo(deque,data[0], range[0], range[1]);
	                    System.out.println();
	                    invest(deque, range[1]);
	                }
                }
                else {
                	System.out.println();
                }
            }

            in.close();
            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
        
    }
    
    //print name 2.2
    static int printName(String ticker) throws SQLException{
    	if(ticker.equals("")) {
    		System.exit(0);;
    	}
    	PreparedStatement pstmt = conn.prepareStatement(
                "select Name" +
                "  from Company " +
                "  where Ticker = ?");
    	
    	pstmt.setString(1, ticker);
    	ResultSet rs = pstmt.executeQuery();
    	if(rs.next()) {
    		System.out.printf("%s%n", rs.getString("Name"));
    		return 1;
    	}
    	else {
    		System.out.printf("%s not found in database%n", ticker);
    		return 0;
    	}
    }
    
    //is year a before string b
    static boolean before(String a, String b) {
    	String[] yeara = a.split("\\.");
    	String[] yearb = b.split("\\.");
    	int yearvala = Integer.parseInt(yeara[0])*365;
    	int yearvalb = Integer.parseInt(yearb[0])*365;
    	int monthvala = Integer.parseInt(yeara[1])*31;
    	int monthvalb = Integer.parseInt(yearb[1])*31;
    	int tvala = Integer.parseInt(yeara[2])+monthvala+yearvala;
    	int tvalb = Integer.parseInt(yearb[2])+monthvalb+yearvalb;
    	if(tvala<=tvalb) {
    		return true;
    	}
    	else {
    		return false;
    	}
    }
    
    //gets min and max dates from a given ticker, used to range data gathering
    static String[] getRange(String ticker) throws SQLException {
    	String [] range = new String[2];
    	PreparedStatement start = conn.prepareStatement(
                "select min(TransDate)" +
                "  from PriceVolume " +
                "  where Ticker = ?");
    	start.setString(1, ticker);
    	PreparedStatement end = conn.prepareStatement(
                "select max(TransDate)" +
                "  from PriceVolume " +
                "  where Ticker = ?");
    	end.setString(1, ticker);
    	
    	ResultSet getstart = start.executeQuery();
    	ResultSet getend = end.executeQuery();
    	if(getstart.next()) {
    		range[0]=getstart.getString(1);
    	}
    	if(getend.next()) {
    		range[1]=getend.getString(1);
    	}
    	
    	System.out.printf("calculated range from %s to %s%n", range[0], range[1]);
    	start.close();
    	end.close();
    	getstart.close();
    	getend.close();
    	return range;
    }
    
    //get info from price volume 2.3
    static void getInfo(Deque<stockDay> deque, String ticker, String start, String end) throws SQLException {
    	PreparedStatement stmt = conn.prepareStatement(
                "select *" +
                "  from PriceVolume " +
                "  where Ticker = ? and TransDate >= ? and TransDate<= ?"+
                "order by TransDate DESC");
    	stmt.setString(1, ticker);
    	stmt.setString(2, start);
    	stmt.setString(3, end);
    	ResultSet rs =stmt.executeQuery();
    	double opening=0.0;
    	double closing = 0.0;
    	adj = 1.0;
    	String date = null;
    	int days =0;
    	int splitTotal=0;
    	stockDay add;
    	
    	while(rs.next()) {
    		days++;
    		if(opening ==0.0) {
    			date =rs.getString("TransDate");
    			opening = rs.getDouble("OpenPrice");
    			
    			add = new stockDay(rs.getString("TransDate"), rs.getDouble("OpenPrice"), rs.getDouble("ClosePrice"),adj);
    			deque.addFirst(add);
    			
    			days++;
    			rs.next();
    		}
    		
    		closing = rs.getDouble("ClosePrice")*adj;
    		date = rs.getString("TransDate");
    		add = new stockDay(rs.getString("TransDate"), rs.getDouble("OpenPrice"), rs.getDouble("ClosePrice"),adj);
    		splitTotal+=stocksplit(date,opening, closing);
    		
    		opening = rs.getDouble("OpenPrice")*adj;
    		deque.push(add);
    		
    		
    	}
    	System.out.printf("%d splits in %d trading days%n", splitTotal, days);
    	stmt.close();
    }
    
  //splits stocks for getinfo function, return 2.1 if 2:1, 3.1 if 3:1, 3.2 if 3:2, and 0 if none, changes the adj that goes into table; 
    static int stocksplit(String date, double opening, double closing) {
    	double two_one= Math.abs(closing/opening-2.0);
    	double three_one= Math.abs(closing/opening-3.0);
    	double three_two= Math.abs(closing/opening-1.5);
    	if(two_one<0.2) {//2:1
    		System.out.printf("2:1 split on %s %.2f-->%.2f%n",date, closing/adj, opening/adj);
    		adj = adj*2;
    		System.out.println("new adj = "+adj);
    		return 1;
    	}
    	if(three_one<0.3) {//3:1
    		System.out.printf("3:1 split on %s %.2f-->%.2f%n",date, closing/adj, opening/adj);
    		adj =  adj*3;
    		System.out.println("new adj = "+adj);
    		return 1;
    	}
    	if(three_two<.15) {//3:2
    		System.out.printf("3:2 split on %s %.2f-->%.2f%n",date, closing/adj, opening/adj);
    		adj = adj*1.5;
    		System.out.println("new adj = "+adj);
    		return 1;
    	}
    	return 0;
    	
    }
    
    //Implements the average finder and investment strategy
    static void invest(Deque<stockDay> data, String endDay){
    	Deque<stockDay> average = new LinkedList<stockDay>();
    	double currentAvg;
    	int transactionCount =0;
    	double cash=0.00;
    	double stocks = 0;
    	boolean buy=false;
    	boolean sell=false;
    	double sellClose =0.0;
    	Iterator <stockDay>iter;
    	System.out.println("Executing investment strategy");
    	//add first 50 items from data to the average deque
    	average.addFirst(data.pop());
    	for(int i=0; i<49; i++) {
    		average.add(data.pop());
    	}
    	//starts calculating wether to buy or sell, iterating through data, adding to tail of 
    	//average while taking away the head of average.
    	while(average.peekLast().date.equals(endDay)==false && data.peekFirst()!=null) {
    		iter = average.iterator();
    		currentAvg = 0;
    		while(iter.hasNext()) {
    			currentAvg += iter.next().getadj();
    		}
    		currentAvg = currentAvg/50;
    		average.add(data.pop());
    		while(average.peekLast().date.equals("1992.07.14")) {
    			System.out.printf(" op: %f , average = %f close[d-1]: %f%n",average.peekLast().opening/ average.peekLast().adjustment, currentAvg, sellClose );
    			break;
    		}
    		
    		if( average.peekLast().closing/ average.peekLast().adjustment< currentAvg  &&  ((average.peekLast().closing / average.peekLast().opening)<= 0.97000001)) {//Buy Criteria	
    			stocks+=100;
    			cash -= 100*data.peek().opening/data.peek().adjustment;
    			cash-= 8.00;
    			transactionCount++;			
    		}
    		if(stocks>=100 && average.peekLast().opening/ average.peekLast().adjustment > currentAvg  &&  ((average.peekLast().opening/ average.peekLast().adjustment) /sellClose >= 1.00999999)) {// Sell Criteria
    			stocks-=100;
    			cash += 100*((average.peekLast().opening/ average.peekLast().adjustment) + (average.peekLast().closing/ average.peekLast().adjustment) / 2);
    			cash -=8.00;
    			transactionCount++;
    		}
    		sellClose = average.peekLast().closing/average.peekLast().adjustment;
    		//get rid of the head of both deques
    		
    		average.pop();
    	}
    	cash+= stocks * (average.peekLast().opening + average.peekLast().closing /2);
    	System.out.printf("transactions executed: %d%n",transactionCount );
    	System.out.printf("Net cash: %.2f%n", cash);
    	System.out.println();
    	
    }

}