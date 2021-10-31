import java.sql.*;
import java.util.*;

public class Query extends QuerySearchOnly {

	// Logged In User
	private String username; // customer username is unique

	// transactions

	private static final String CLEAR_TABLES = "DELETE FROM Reservation; DELETE FROM Users;";
	protected PreparedStatement clearTableStatement;

	private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	protected PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	protected PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	protected PreparedStatement rollbackTransactionStatement;

	private static final String CHECK_USER_EXISTS = "SELECT COUNT(*) AS count FROM Users AS U WHERE U.username = ?";
	protected PreparedStatement checkUserExistsStatement;

	private static final String CREATE_USER = "INSERT INTO Users VALUES(?, ?, ?)";
	protected PreparedStatement createUserStatement;

	private static final String CHECK_LOGIN = "SELECT U.username AS username FROM Users AS U WHERE "
			+ "U.username = ? AND U.password = ? ";
	protected PreparedStatement checkLoginStatement;

	private static final String CHECK_RESERVATION_DATE = "SELECT R.username AS username, R.day AS day "
			+ "FROM Reservation AS R WHERE R.username = ? AND R.day = ? ";
	protected PreparedStatement checkReservationDateStatement;

	private static final String CHECK_AVAILABLE_SEAT1 = "SELECT COUNT(*) AS count FROM Reservation as R " +
			"WHERE R.fid1 = ? AND canceled = 0";
	protected PreparedStatement checkAvailableSeat1Statement;

	private static final String CHECK_AVAILABLE_SEAT2 = "SELECT COUNT(*) AS count FROM Reservation as R " +
			"WHERE R.fid2 = ? AND canceled = 0";
	protected PreparedStatement checkAvailableSeat2Statement;

	private static final String GENERATE_RESERVATION_ID = "SELECT MAX(R.reservationId) AS id FROM Reservation AS R";
	protected PreparedStatement generateReservationIdStatement;

	private static final String MAKE_RESERVATION = "INSERT INTO Reservation VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
	protected PreparedStatement makeReservationStatement;

    private static final String MAKE_PAYMENT = "SELECT * FROM Reservation AS R WHERE R.username = ? " +
            "AND R.reservationId = ? AND R.paid = 'false'";
    protected PreparedStatement makePaymentStatement;

    private static final String CHECK_BALANCE = "SELECT U.balance AS balance FROM Users AS U WHERE U.username = ?";
    protected PreparedStatement checkBalanceStatement;

	private static final String UPDATE_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
	protected PreparedStatement updateBalanceStatement;

	private static final String UPDATE_PAID = "UPDATE Reservation SET paid = 'true' WHERE reservationId = ?";
	protected PreparedStatement updatePaidStatement;

	private static final String SHOW_RESERVATION = "SELECT * FROM Reservation WHERE username = ?";
	protected PreparedStatement showReservationStatement;

	private static final String SEARCH_FLIGHT = "SELECT F.day_of_month AS Day1, "
			+ "F.carrier_id AS Carrier1, F.flight_num AS Number1, F.fid AS fid1, "
			+ "F.origin_city AS Origin1, F.dest_city AS Destination1, "
			+ "F.actual_time AS Duration1, F.capacity AS Capacity1, F.price AS Price1 "
			+ "FROM FLIGHTS AS F "
			+ "WHERE F.fid = ?";
	protected PreparedStatement searchFlightStatement;

	private static final String CHECK_CANCELED_RESERVATION = "SELECT canceled FROM Reservation " +
			"WHERE reservationId = ? AND canceled = 0";
	protected PreparedStatement checkCanceledReservationStatement;

	private static final String CANCEL_RESERVATION = "UPDATE Reservation SET canceled = 1, " +
			"paid = 'true' WHERE reservationId = ?";
	protected PreparedStatement cancelReservationStatement;

	public Query(String configFilename) {
		super(configFilename);
	}


	/**
	 * Clear the data in any custom tables created. Do not drop any tables and do not
	 * clear the flights table. You should clear any tables you use to store reservations
	 * and reset the next reservation ID to be 1.
	 */
	public void clearTables ()
	{
		// your code here
		try {
			clearTableStatement.executeUpdate();
		} catch (SQLException e) { e.printStackTrace();}
	}


	/**
	 * prepare all the SQL statements in this method.
	 * "preparing" a statement is almost like compiling it.
	 * Note that the parameters (with ?) are still not filled in
	 */
	@Override
	public void prepareStatements() throws Exception
	{
		super.prepareStatements();
		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */

		clearTableStatement = conn.prepareStatement(CLEAR_TABLES);

		// For transaction_createCustomer method
		checkUserExistsStatement = conn.prepareStatement(CHECK_USER_EXISTS);
		createUserStatement = conn.prepareStatement(CREATE_USER);

		// For transaction_login method
		checkLoginStatement = conn.prepareStatement(CHECK_LOGIN);

		// For transaction_book method
		checkReservationDateStatement = conn.prepareStatement(CHECK_RESERVATION_DATE);
		checkAvailableSeat1Statement = conn.prepareStatement(CHECK_AVAILABLE_SEAT1);
		checkAvailableSeat2Statement = conn.prepareStatement(CHECK_AVAILABLE_SEAT2);
		generateReservationIdStatement = conn.prepareStatement(GENERATE_RESERVATION_ID);
		makeReservationStatement = conn.prepareStatement(MAKE_RESERVATION);

		// For transaction_pay method
        makePaymentStatement = conn.prepareStatement(MAKE_PAYMENT);
        checkBalanceStatement = conn.prepareStatement(CHECK_BALANCE);
        updateBalanceStatement = conn.prepareStatement(UPDATE_BALANCE);
        updatePaidStatement = conn.prepareStatement(UPDATE_PAID);

        // For transaction_reservations method
		showReservationStatement = conn.prepareStatement(SHOW_RESERVATION);
		searchFlightStatement = conn.prepareStatement(SEARCH_FLIGHT);

		// For transaction_cancel method
		checkCanceledReservationStatement = conn.prepareStatement(CHECK_CANCELED_RESERVATION);
		cancelReservationStatement = conn.prepareStatement(CANCEL_RESERVATION);
	}


	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @return If someone has already logged in, then return "User already logged in\n"
	 * For all other errors, return "Login failed\n".
	 *
	 * Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password)
	{
		if (this.username != null) {
			return "User already logged in\n";
		}
		try {
			beginTransaction();
            checkLoginStatement.clearParameters();
            checkLoginStatement.setString(1, username);
            checkLoginStatement.setString(2, password);
            ResultSet check = checkLoginStatement.executeQuery();
            if (check.next()) { // Check identity
				if (check.getString("username").equals(username)) {
					this.username = username; // Login this user
					commitTransaction();
					return "Logged in as " + username + "\n";
				}
            }
            rollbackTransaction();
			return "Login failed\n";
        } catch (SQLException e) {
			return "Login failed\n";
        }
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username new user's username. User names are unique the system.
	 * @param password new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
	 *
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 */
	public String transaction_createCustomer(String username, String password, int initAmount)
	{
		if (initAmount < 0) {
			return "Failed to create user\n";
		} try {
			beginTransaction();
			checkUserExistsStatement.clearParameters();
			checkUserExistsStatement.setString(1, username);
			ResultSet check = checkUserExistsStatement.executeQuery();
			check.next();
			if (check.getInt("count") == 0) { // This username has not been created before
				createUserStatement.clearParameters();
				createUserStatement.setString(1, username);
				createUserStatement.setString(2, password);
				createUserStatement.setInt(3, initAmount);
				try {
					createUserStatement.executeUpdate();
					commitTransaction();
					return "Created user " + username + "\n";
				} catch (SQLException e) {
					return transaction_createCustomer(username, password, initAmount);
				}
			}
			rollbackTransaction();
            return "Failed to create user\n";
		} catch (SQLException e) {
			return "Failed to create user\n";
		}
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
	 *
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
	 * If the user already has a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n".
	 * For all other errors, return "Booking failed\n".
	 *
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
	 * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
	 * successful reservation is made by any user in the system.
	 */
	public String transaction_book(int itineraryId)
	{
		// Check if the user is logged in
        List<Itinerary> searchResult = getSearchResult();
		if (this.username == null) {
			return "Cannot book reservations, not logged in\n";
		} else if (searchResult == null || itineraryId > searchResult.size() - 1 || itineraryId < 0) {
			return "No such itinerary " + itineraryId + "\n";
		}
		try {
			// Start booking
            Itinerary trip = searchResult.get(itineraryId);
            int date = trip.getDate();
			beginTransaction();
			// Check if user have reservation on the same day
			checkReservationDateStatement.clearParameters();
			checkReservationDateStatement.setString(1, username);
			checkReservationDateStatement.setInt(2, date);
			try {
				ResultSet checkDate = checkReservationDateStatement.executeQuery();
				if (checkDate.next()) { // This user has a reservation on the same date
					rollbackTransaction();
					return "You cannot book two flights in the same day\n";
				}
			} catch (SQLException deadlock) {
				return transaction_book(itineraryId);
			}

			// Check if there's seat avaliable for both f1 and f2 (if indirect)
			Flight flight1 = trip.getFlight(0);
			int fid1 = flight1.fid;
			Flight flight2 = null;
			int fid2 = 0;
			try {
                checkAvailableSeat1Statement.clearParameters();
				checkAvailableSeat1Statement.setInt(1, fid1);
				ResultSet reservedSeatCount1 = checkAvailableSeat1Statement.executeQuery();
				reservedSeatCount1.next();
				if (reservedSeatCount1.getInt("count") == flight1.capacity) { // No seat avaliable for flight 1
                    rollbackTransaction();
					return "Booking failed\n";
				} else if (trip.size() == 2) {
                    flight2 = trip.getFlight(1);
                    fid2 = flight2.fid;
					checkAvailableSeat2Statement.clearParameters();
					checkAvailableSeat2Statement.setInt(1, fid2);
					ResultSet reservedSeatCount2 = checkAvailableSeat2Statement.executeQuery();
					reservedSeatCount2.next();
					if (reservedSeatCount2.getInt("count") == flight2.capacity) { // No seat avaliable for flight 2
                        rollbackTransaction();
						return "Booking failed\n";
					}
				}
			} catch (SQLException deadlock) {
				return transaction_book(itineraryId);
			}

			// User is allowed to book this reservation
			// Generate a new reservation ID
			int newReservationID = 0;
			try {
				ResultSet reservedId = generateReservationIdStatement.executeQuery();
				if (reservedId.next()) {
                    newReservationID = reservedId.getInt("id") + 1; // Increment by one: ensures unique
                }
				try {
					makeReservationStatement.clearParameters();
					makeReservationStatement.setInt(1, newReservationID);
					makeReservationStatement.setString(2, this.username);
					makeReservationStatement.setInt(3, date);
					makeReservationStatement.setInt(4, fid1);
					makeReservationStatement.setInt(5, fid2);
					makeReservationStatement.setInt(6, trip.totalPrice);
					makeReservationStatement.setString(7, "false");
					makeReservationStatement.setInt(8, 0);
					makeReservationStatement.executeUpdate();
					commitTransaction();
					return "Booked flight(s), reservation ID: " + newReservationID + "\n";
				} catch (SQLException deadlock) {
					return transaction_book(itineraryId);
				}
			} catch (SQLException deadlock) {
				return transaction_book(itineraryId);
			}
		} catch (SQLException e) {
			return "Booking failed\n";
		}
	}

	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 *
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n"
	 * If the reservation is not found / not under the logged in user's name, then return
	 * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
	 * If the user does not have enough money in their account, then return
	 * "User has only [balance] in account but itinerary costs [cost]\n"
	 * For all other errors, return "Failed to pay for reservation [reservationId]\n"
	 *
	 * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
	 * where [balance] is the remaining balance in the user's account.
	 */
	public String transaction_pay (int reservationId)
	{
		if (this.username == null) {
		    return "Cannot pay, not logged in\n";
        }
		try {
		    // Check reservation id matches given id
            beginTransaction();
            makePaymentStatement.clearParameters();
            makePaymentStatement.setString(1, this.username);
            makePaymentStatement.setInt(2, reservationId);
            ResultSet reservations = makePaymentStatement.executeQuery();
            if (reservations.next()) {
                int price = reservations.getInt("price");
                // Check user balance
                checkBalanceStatement.clearParameters();
                checkBalanceStatement.setString(1, this.username);
                ResultSet checkBalance = checkBalanceStatement.executeQuery();
                checkBalance.next();
                int balance = checkBalance.getInt("balance");
                if (price > balance) {
                	rollbackTransaction();
                    return "User has only " + balance + " in account but itinerary costs " + price + "\n";
                } else {
                	// Pay the money
                	balance -= price;
                	updateBalanceStatement.clearParameters();
                	updateBalanceStatement.setInt(1, balance);
                	updateBalanceStatement.setString(2, this.username);
                	updateBalanceStatement.executeUpdate();
                	// Change reservation from unpaid to paid
					updatePaidStatement.clearParameters();
					updatePaidStatement.setInt(1, reservationId);
                	updatePaidStatement.executeUpdate();
                	commitTransaction();
					return "Paid reservation: " + reservationId + " remaining balance: " + balance + "\n";
                }
            }
            rollbackTransaction();
            return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";

		} catch (SQLException e) {
			return "Failed to cancel reservation " + reservationId + "\n";
        }
	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
	 * If the user has no reservations, then return "No reservations found\n"
	 * For all other errors, return "Failed to retrieve reservations\n"
	 *
	 * Otherwise return the reservations in the following format:
	 *
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * ...
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 *
	 * @see Flight#toString()
	 */
	public String transaction_reservations()
	{
		if (username == null) {
			return "Cannot view reservations, not logged in\n";
		}
		try {
			StringBuffer sb = new StringBuffer();
			showReservationStatement.clearParameters();
			showReservationStatement.setString(1, this.username);
			ResultSet reservations = showReservationStatement.executeQuery();
			while (reservations.next()) {
				sb.append("Reservation " + reservations.getInt("reservationID") + " paid: "
						+ reservations.getString("paid") + ":\n");
				int fid1 = reservations.getInt("fid1");
				int fid2 = reservations.getInt("fid2");
				// Flight 1: add to string buffer
				searchFlightStatement.clearParameters();
				searchFlightStatement.setInt(1, fid1);
				ResultSet flight1 = searchFlightStatement.executeQuery();
				flight1.next();
				Flight f1 = new Flight(flight1, 1);
				sb.append(f1.toString() + "\n");
				// Flight 2: check whether add to string buffer
				if (fid2 != 0) {
					searchFlightStatement.clearParameters();
					searchFlightStatement.setInt(1, fid2);
					ResultSet flight2 = searchFlightStatement.executeQuery();
					flight2.next();
					Flight f2 = new Flight(flight2, 1);
					sb.append(f2.toString() + "\n");
				}
			}
			String result = sb.toString();
			if (result.isEmpty()) {
				return "No reservations found\n";
			} else {
				return result;
			}
		} catch (SQLException e) {
			return "Failed to retrieve reservations\n";
		}
	}

	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 *
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
	 * For all other errors, return "Failed to cancel reservation [reservationId]"
	 *
	 * If successful, return "Canceled reservation [reservationId]"
	 *
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId)
	{
		if (this.username == null) {
			return "Cannot cancel reservations, not logged in\n";
		}
		try {
			beginTransaction();
			checkCanceledReservationStatement.clearParameters();
			checkCanceledReservationStatement.setInt(1, reservationId);
			ResultSet checkCanceled = checkCanceledReservationStatement.executeQuery();
			if (checkCanceled.next()) {
				// Able to canceled
				cancelReservationStatement.clearParameters();
				cancelReservationStatement.setInt(1, reservationId);
				cancelReservationStatement.executeUpdate();
				commitTransaction();
				return "Canceled reservation " + reservationId + "\n";
			}
			rollbackTransaction();
			return "Failed to cancel reservation " + reservationId + "\n";
		} catch (SQLException e) {
			return "Failed to cancel reservation " + reservationId + "\n";
		}
	}


	/* some utility functions below */
	public void beginTransaction() throws SQLException
	{
		conn.setAutoCommit(false);
		beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException
	{
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException
	{
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}
}
