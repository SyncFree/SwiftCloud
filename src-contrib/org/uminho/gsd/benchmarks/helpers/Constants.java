

package org.uminho.gsd.benchmarks.helpers;

 /**
 * Useful constants.
 *
 * @author <a href="mailto:totok@cs.nyu.edu">Alexander Totok</a>
 *
 * @version   $Revision: 1.4 $   $Date: 2005/02/05 21:26:28 $   $Author: totok $
 */
public class Constants {
	
	public static final String driverName = "com.mysql.jdbc.Driver";
	public static final String dbName = "jdbc:mysql://192.168.82.20:3306/TPCW";
	


	//ATTENTION: The NUM_EBS and NUM_ITEMS variables are the only variables
	//that should be modified in order to rescale the DB.
	/**
	 * Number of EBs - Emulated Browsers
	 */
	public static final int NUM_EBS = 10;
	/**
	 * Number of Items in the database
	 */
	public static final int NUM_ITEMS = 1000;
	/**
	 * Number of images pre-generated and kept in the WAR archive. May be less than NUM_ITEMS, actually.
	 */
	public static final int NUM_IMAGES = 100;
	// ---- ATTENTION ----



	public static final int NUM_CUSTOMERS = NUM_EBS * 2880;
	public static final int NUM_COUNTRIES = 92;
	public static final int NUM_ADDRESSES = 2 * NUM_CUSTOMERS;
	public static final int NUM_ORDERS = (int)(0.9 * NUM_CUSTOMERS);
	public static final int NUM_AUTHORS = (int) (0.25 * NUM_ITEMS);
	public static final int NUM_CC_XACTS = NUM_ORDERS;
	
	public static final String[] tables = {"address", "author", "cc_xacts", "country", "customer", "item",
											"order_line", "orders", "ids"};
	public static final int NUM_TABLES = tables.length;

	public static final String CREATE_ADDRESS_TABLE = "CREATE TABLE address ( ADDR_ID int not null, ADDR_STREET1 varchar(40), ADDR_STREET2 varchar(40), ADDR_CITY varchar(30), ADDR_STATE varchar(20), ADDR_ZIP varchar(10), ADDR_CO_ID int, PRIMARY KEY(ADDR_ID)) TYPE = InnoDB";
	public static final String CREATE_AUTHOR_TABLE = "CREATE TABLE author ( A_ID int not null, A_FNAME varchar(20), A_LNAME varchar(20), A_MNAME varchar(20), A_DOB datetime, A_BIO varchar(255), PRIMARY KEY(A_ID)) TYPE = InnoDB"; // A_BIO varchar(500)
	public static final String CREATE_COUNTRY_TABLE = "CREATE TABLE country ( CO_ID int not null, CO_NAME varchar(50), CO_EXCHANGE double, CO_CURRENCY varchar(18), PRIMARY KEY(CO_ID)) TYPE = InnoDB";
	public static final String CREATE_CUSTOMER_TABLE = "CREATE TABLE customer ( C_ID int not null, C_UNAME varchar(20), C_PASSWD varchar(20), C_FNAME varchar(15), C_LNAME varchar(15), C_ADDR_ID int, C_PHONE varchar(16), C_EMAIL varchar(50), C_SINCE datetime, C_LAST_VISIT datetime, C_LOGIN datetime, C_EXPIRATION datetime, C_DISCOUNT double, C_BALANCE double, C_YTD_PMT double, C_BIRTHDATE datetime, C_DATA varchar(255), PRIMARY KEY(C_ID)) TYPE = InnoDB"; // C_DATA varchar(500)
	public static final String CREATE_ITEM_TABLE = "CREATE TABLE item ( I_ID int not null, I_TITLE varchar(60), I_A_ID int, I_PUB_DATE datetime, I_PUBLISHER varchar(60), I_SUBJECT varchar(60), I_DESC varchar(255), I_RELATED1 int, I_RELATED2 int, I_RELATED3 int, I_RELATED4 int, I_RELATED5 int, I_THUMBNAIL varchar(40), I_IMAGE varchar(40), I_SRP double, I_COST double, I_AVAIL datetime, I_STOCK int, I_ISBN char(13), I_PAGE int, I_BACKING varchar(15), I_DIMENSIONS varchar(25), PRIMARY KEY(I_ID)) TYPE = InnoDB"; // I_DESC varchar(500)
	public static final String CREATE_ORDER_LINE_TABLE = "CREATE TABLE order_line ( OL_ID int not null, OL_O_ID int not null, OL_I_ID int, OL_QTY int, OL_DISCOUNT double, OL_COMMENTS varchar(100), PRIMARY KEY(OL_O_ID, OL_ID)) TYPE = InnoDB";
	public static final String CREATE_ORDERS_TABLE = "CREATE TABLE orders ( O_ID int not null, O_C_ID int, O_DATE datetime, O_SUB_TOTAL double, O_TAX double, O_TOTAL double, O_SHIP_TYPE varchar(10), O_SHIP_DATE datetime, O_BILL_ADDR_ID int, O_SHIP_ADDR_ID int, O_STATUS varchar(15), PRIMARY KEY(O_ID)) TYPE = InnoDB";
	public static final String CREATE_CC_XACTS_TABLE = "CREATE TABLE cc_xacts ( CX_O_ID int not null, CX_TYPE varchar(10), CX_NUM varchar(16), CX_NAME varchar(31), CX_EXPIRE datetime, CX_AUTH_ID char(15), CX_XACT_AMT double, CX_XACT_DATE datetime, CX_CO_ID int, PRIMARY KEY(CX_O_ID)) TYPE = InnoDB";
	public static final String CREATE_IDS_TABLE = "CREATE TABLE ids (id int not null, address_id int not null, customer_id int not null, order_id int not null, PRIMARY KEY(id)) TYPE = InnoDB";
	public static final String INSERT_IDS = "INSERT into ids (id, address_id, customer_id, order_id) VALUES(1, " + NUM_ADDRESSES + ", " + NUM_CUSTOMERS + ", " + NUM_ORDERS + ")";
	
	public static final String[] COUNTRIES = {"United States","United Kingdom","Canada",
				  "Germany","France","Japan","Netherlands",
				  "Italy","Switzerland", "Australia","Algeria",
				  "Argentina","Armenia","Austria","Azerbaijan",
				  "Bahamas","Bahrain","Bangla Desh","Barbados",
				  "Belarus","Belgium","Bermuda", "Bolivia",
				  "Botswana","Brazil","Bulgaria","Cayman Islands",
				  "Chad","Chile", "China","Christmas Island",
				  "Colombia","Croatia","Cuba","Cyprus",
				  "Czech Republic","Denmark","Dominican Republic",
				  "Eastern Caribbean","Ecuador", "Egypt",
				  "El Salvador","Estonia","Ethiopia",
				  "Falkland Island","Faroe Island", "Fiji", 
				  "Finland","Gabon","Gibraltar","Greece","Guam",
				  "Hong Kong","Hungary", "Iceland","India",
				  "Indonesia","Iran","Iraq","Ireland","Israel",
				  "Jamaica", "Jordan","Kazakhstan","Kuwait",
				  "Lebanon","Luxembourg","Malaysia","Mexico", 
				  "Mauritius", "New Zealand","Norway","Pakistan",
				  "Philippines","Poland","Portugal","Romania", 
				  "Russia","Saudi Arabia","Singapore","Slovakia",
				  "South Africa","South Korea", "Spain","Sudan",
				  "Sweden","Taiwan","Thailand","Trinidad",
				  "Turkey","Venezuela", "Zambia"};

	public static final double[] EXCHANGES = { 1, .625461, 1.46712, 1.86125, 6.24238, 121.907,
				   2.09715, 1842.64, 1.51645, 1.54208, 65.3851,
				   0.998, 540.92, 13.0949, 3977, 1, .3757, 
				   48.65, 2, 248000, 38.3892, 1, 5.74, 4.7304,
				   1.71, 1846, .8282, 627.1999, 494.2, 8.278,
				   1.5391, 1677, 7.3044, 23, .543, 36.0127, 
				   7.0707, 15.8, 2.7, 9600, 3.33771, 8.7,
				   14.9912, 7.7, .6255, 7.124, 1.9724, 5.65822,
				   627.1999, .6255, 309.214, 1, 7.75473, 237.23, 
				   74.147, 42.75, 8100, 3000, .3083, .749481,
				   4.12, 37.4, 0.708, 150, .3062, 1502, 38.3892,
				   3.8, 9.6287, 25.245, 1.87539, 7.83101,
				   52, 37.8501, 3.9525, 190.788, 15180.2, 
				   24.43, 3.7501, 1.72929, 43.9642, 6.25845, 
				   1190.15, 158.34, 5.282, 8.54477, 32.77, 37.1414,
				   6.1764, 401500, 596, 2447.7 };

	public static final String[] CURRENCIES = { "Dollars","Pounds","Dollars","Deutsche Marks",
				"Francs","Yen","Guilders","Lira","Francs",
				"Dollars","Dinars","Pesos", "Dram",
				"Schillings","Manat","Dollars","Dinar","Taka",
				"Dollars","Rouble","Francs","Dollars", 
				"Boliviano", "Pula", "Real", "Lev","Dollars",
				"Franc","Pesos","Yuan Renmimbi","Dollars",
				"Pesos","Kuna","Pesos","Pounds","Koruna",
				"Kroner","Pesos","Dollars","Sucre","Pounds",
				"Colon","Kroon","Birr","Pound","Krone",
				"Dollars","Markka","Franc","Pound","Drachmas",
				"Dollars","Dollars","Forint","Krona","Rupees",
				"Rupiah","Rial","Dinar","Punt","Shekels",
				"Dollars","Dinar","Tenge","Dinar","Pounds",
				"Francs","Ringgit","Pesos","Rupees","Dollars",
				"Kroner","Rupees","Pesos","Zloty","Escudo",
				"Leu","Rubles","Riyal","Dollars","Koruna",
				"Rand","Won","Pesetas","Dinar","Krona",
				"Dollars","Baht","Dollars","Lira","Bolivar", 
				"Kwacha"};

	public static final char[] CHARS = {'a','b','c','d','e','f','g','h','i','j','k',
			  'l','m','n','o','p','q','r','s','t','u','v',
			  'w','x','y','z','A','B','C','D','E','F','G',
			  'H','I','J','K','L','M','N','O','P','Q','R',
			  'S','T','U','V','W','X','Y','Z','!','@','#',
			  '$','%','^','&','*','(',')','_','-','=','+',
			  '{','}','[',']','|',':',';',',','.','?','/',
			  '~',' ','0','1','2','3','4','5','6','7','8','9'};
	public static final int NUM_CHARS = CHARS.length;
	
	public static final char[] NUMBERS = {'0','1','2','3','4','5','6','7','8','9'};

	public static final int NUM_SUBJECTS = 24;
	public static final String[] SUBJECTS = { "ARTS", "BIOGRAPHIES", "BUSINESS", "CHILDREN",
				  "COMPUTERS", "COOKING", "HEALTH", "HISTORY",
				  "HOME", "HUMOR", "LITERATURE", "MYSTERY",
				  "NON-FICTION", "PARENTING", "POLITICS",
				  "REFERENCE", "RELIGION", "ROMANCE", 
				  "SELF-HELP", "SCIENCE-NATURE", "SCIENCE_FICTION",
				  "SPORTS", "YOUTH", "TRAVEL"};

	public static final int NUM_BACKINGS = 5;	
	public static final String[] BACKINGS = { "HARDBACK", "PAPERBACK", "USED", "AUDIO", "LIMITED-EDITION"};

	public static final int NUM_CARD_TYPES = 5;
	public static final String[] CARD_TYPES = {"VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS"};

	public static final int NUM_SHIP_TYPES = 6;
	public static final String[] SHIP_TYPES = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
	
	public static final int NUM_STATUS_TYPES = 4;
	public static final String[] STATUS_TYPES = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};
	
	public static final String I_TITLE_FILE = "I_TITLE.TXT";
	public static final String A_LNAME_FILE = "A_LNAME.TXT";

	public static final int SEARCH_LIMIT = 50;
	public static final int RECENT_ORDERS_LIMIT = 3333;
	
	public static final String CART = "cart";
	public static final String C_ID = "C_ID";
	public static final String C_UNAME = "C_UNAME";
}
