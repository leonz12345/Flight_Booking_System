import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

import java.util.*;
/**
 * Runs queries against a back-end database.
 * This class is responsible for searching for flights.
 */
public class QuerySearchOnly
{
  // `dbconn.properties` config file
  private String configFilename;
  private List<Itinerary> searchResult;

  // DB Connection
  protected Connection conn;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  protected PreparedStatement checkFlightCapacityStatement;

  private static final String SEARCH_DIRECT_FLIGHT = "SELECT F.day_of_month as Day, "
          + "F.carrier_id as Carrier, F.flight_num as Number, F.fid as fid, "
          + "F.origin_city as Origin, F.dest_city as Destination, "
          + "F.actual_time as Duration, F.capacity as Capacity, F.price as Price "
          + "FROM FLIGHTS as F "
          + "WHERE F.origin_city = ? AND F.dest_city = ? AND F.day_of_month = ? "
          + "AND F.canceled != 1 "
          + "ORDER BY F.actual_time ASC, F.fid ASC";
  protected PreparedStatement searchDirectFlightStatement;

  private static final String SEARCH_INDIRECT_FLIGHT = "SELECT F1.day_of_month as Day1, "
          + "F1.carrier_id as Carrier1, F1.flight_num as Number1, F1.origin_city as Origin1, "
          + "F1.dest_city as Destination1, F1.actual_time as Duration1, F1.capacity as Capacity1, "
          + "F1.price as Price1, F2.day_of_month as Day2, F2.carrier_id as Carrier2, "
          + "F2.flight_num as Number2, F2.origin_city as Origin2, F2.dest_city as Destination2, "
          + "F2.actual_time as Duration2, F2.capacity as Capacity2, F2.price as Price2, "
          + "F1.fid as fid1, F2.fid as fid2, F1.actual_time + F2.actual_time as Total_time "
          + "FROM FLIGHTS as F1, FLIGHTS as F2 "
          + "WHERE F1.origin_city = ? AND F1.dest_city = F2.origin_city AND F2.dest_city = ? "
          + "AND F1.day_of_month = ? AND F2.day_of_month = F1.day_of_month AND F1.canceled != 1 "
          + "AND F2.canceled != 1 "
          + "ORDER BY Total_time ASC , F1.fid ASC, F2.fid ASC";
  protected PreparedStatement searchIndirectFlightStatement;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    public Flight(ResultSet results, int n) {
      try {
        this.capacity = results.getInt("Capacity" + n);
        this.carrierId = results.getString("Carrier" + n);
        this.dayOfMonth = results.getInt("Day" + n);
        this.destCity = results.getString("Destination" + n);
        this.fid = results.getInt("fid" + n);
        this.flightNum = results.getString("Number" + n);
        this.originCity = results.getString("Origin" + n);
        this.price = results.getInt("Price" + n);
        this.time = results.getInt("Duration" + n);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    public Flight(ResultSet results) {
      try {
        this.capacity = results.getInt("Capacity");
        this.carrierId = results.getString("Carrier");
        this.dayOfMonth = results.getInt("Day");
        this.destCity = results.getString("Destination");
        this.fid = results.getInt("fid");
        this.flightNum = results.getString("Number");
        this.originCity = results.getString("Origin");
        this.price = results.getInt("Price");
        this.time = results.getInt("Duration");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }

  }

  public class Itinerary implements Comparable<Itinerary> {
    public int flightCount;
    public int totalTimeMin;
    public int totalPrice;
    public List<Flight> trip;

    public Itinerary() {
      this.trip = new ArrayList<Flight>();
      this.flightCount = 0;
      this.totalTimeMin = 0;
      this.totalPrice = 0;
    }

    public void add(Flight flight) {
      this.trip.add(flight);
      this.flightCount++;
      this.totalTimeMin += flight.time;
      this.totalPrice += flight.price;
    }

    // WRONG
    public String toString(int itineraryNumber) {
      String result = "Itinerary " + itineraryNumber + ": " + flightCount + " flight(s), " + totalTimeMin + " minutes\n";
      for (Flight current : trip) {
        result += current.toString();
        result += "\n";
      }
      return result;
    }

    public int compareTo(Itinerary other) {
      if (this.totalTimeMin < other.totalTimeMin) {
        return -1;
      } else if (this.totalTimeMin > other.totalTimeMin) {
        return 1;
      } else { // Tie in flight time
        // Break tie by comparing flight count
        if (this.flightCount < other.flightCount) {
          return -1;
        } else if (this.flightCount > other.flightCount) {
          return 1;
        }
      }
      return 0;
    }

    public Flight getFlight(int n) {
      return trip.get(n);
    }

    public int getDate() {
      return trip.get(0).dayOfMonth;
    }

    public int size() {
      return trip.size();
    }
  }

  public QuerySearchOnly(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /** Open a connection to SQL Server in Microsoft Azure.  */
  public void openConnection() throws Exception
  {
    Properties configProps = new Properties();
    configProps.load(new FileInputStream(configFilename));

    String jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    String jSQLUrl = configProps.getProperty("flightservice.url");
    String jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    String jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement
    /* In the full Query class, you will also want to appropriately set the transaction's isolation level:
          conn.setTransactionIsolation(...)
       See Connection class's JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */

    searchDirectFlightStatement = conn.prepareStatement(SEARCH_DIRECT_FLIGHT);
    searchIndirectFlightStatement = conn.prepareStatement(SEARCH_INDIRECT_FLIGHT);
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise it searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    // Please implement your own (safe) version that uses prepared statements rather than string concatenation.
    // You may use the `Flight` class (defined above).


    StringBuffer sb = new StringBuffer();
    List<Itinerary> outputs = new LinkedList<Itinerary>();

    try
    {
      // Add the parameters into Search Direct Flight canned query.
      searchDirectFlightStatement.clearParameters();
      searchDirectFlightStatement.setString(1, originCity);
      searchDirectFlightStatement.setString(2, destinationCity);
      searchDirectFlightStatement.setInt(3, dayOfMonth);
      ResultSet directFlights = searchDirectFlightStatement.executeQuery();

      // Add up to numberOfItineraries direct itineraries to StringBuffer for later return
      while (directFlights.next() && numberOfItineraries != 0) {
        Flight f = new Flight(directFlights); // Convert ResultSet into Flight object
        Itinerary wholeTrip = new Itinerary();
        wholeTrip.add(f);
        outputs.add(wholeTrip);
        numberOfItineraries--;
      }
      directFlights.close();
      // If user want to inculde indirect flight, we need to fill n-k number of indirect flight
      if (!directFlight && numberOfItineraries != 0) {
        // Add the parameters into Search Indirect Flight canned query.
        searchIndirectFlightStatement.clearParameters();
        searchIndirectFlightStatement.setString(1, originCity);
        searchIndirectFlightStatement.setString(2, destinationCity);
        searchIndirectFlightStatement.setInt(3, dayOfMonth);
        ResultSet indirectFlights = searchIndirectFlightStatement.executeQuery();
        while (indirectFlights.next() && numberOfItineraries != 0) {
          // Need 2 flights: Convert ResultSet into 2 Flight objects
          Flight f1 = new Flight(indirectFlights, 1);
          Flight f2 = new Flight(indirectFlights, 2);
          // Create an Itinerary object and put the flights into it, then puti it into output
          Itinerary wholeTrip = new Itinerary();
          wholeTrip.add(f1);
          wholeTrip.add(f2);
          outputs.add(wholeTrip);
          // Update the number of itineraries need to output
          numberOfItineraries--;
        }
        indirectFlights.close();
      }
    } catch (SQLException e) { return "Failed to search\n"; } // e.printStackTrace();
    if (outputs.isEmpty()) {
      return "No flights match your selection\n";
    }
    Collections.sort(outputs);
    searchResult = outputs;
    int itineraryNumber = 0;
    for (Itinerary current : outputs) {
      String trip = current.toString(itineraryNumber);
      sb.append(trip);
      itineraryNumber++;
    }
    return sb.toString();

    //return transaction_search_unsafe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();
    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: ").append(result_dayOfMonth)
                .append(" Carrier: ").append(result_carrierId)
                .append(" Number: ").append(result_flightNum)
                .append(" Origin: ").append(result_originCity)
                .append(" Destination: ").append(result_destCity)
                .append(" Duration: ").append(result_time)
                .append(" Capacity: ").append(result_capacity)
                .append(" Price: ").append(result_price)
                .append('\n');
      }
      oneHopResults.close();
    } catch (SQLException e) { e.printStackTrace(); }

    return sb.toString();
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments.
   * You don't need to use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();
    return capacity;
  }

  public List<Itinerary> getSearchResult() {
    return searchResult;
  }
}
