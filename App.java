import java.sql.*;
import org.postgresql.geometric.PGpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class App {
    
    // A connection to the database
    Connection connection;
    
    String query;
    PreparedStatement ps;
    ResultSet result;
    
    App() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Connects and sets the search path.
     *
     * Establishes a connection to be used for this session, assigning it to
     * the instance variable 'connection'.  In addition, sets the search
     * path to uber.
     *
     * @param  url       the url for the database
     * @param  username  the username to connect to the database
     * @param  password  the password to connect to the database
     * @return           true if connecting is successful, false otherwise
     */
    public boolean connectDB(String URL, String username, String password) {
        
        try {
            connection = DriverManager.getConnection(URL, username, password);
            query = "set search_path to uber, public;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            // connection successful
            return true;
            
        } catch (SQLException se) {
            return false;
        }
        
    }
    
    /**
     * Closes the database connection.
     *
     * @return true if the closing was successful, false otherwise
     */
    public boolean disconnectDB() {
        
        try {
            connection.close();
            return true;
        } catch (SQLException se) {
            return false;
        }
        
    }
    
    /* ======================= Driver-related methods ======================= */
    
    /**
     * Records the fact that a driver has declared that he or she is available
     * to pick up a client.
     *
     * Does so by inserting a row into the Available table.
     *
     * @param  driverID  id of the driver
     * @param  when      the date and time when the driver became available
     * @param  location  the coordinates of the driver at the time when
     *                   the driver became available
     * @return           true if the insertion was successful, false otherwise.
     */
    public boolean available(int driverID, Timestamp when, PGpoint location) {
        
        try {
            
            query = "INSERT INTO Available VALUES (?, ?, ?);";
            ps = connection.prepareStatement(query);
            ps.setInt(1, driverID);
            ps.setTimestamp(2, when);
            ps.setObject(3, location);
            
            ps.execute();
            
            return true;
            
        } catch (SQLException se) {
            se.printStackTrace();
            return false;
        }
        
    }
    
    /**
     * Returns the requestID of a request made by a client and for which a
     * particular was dispatched if driver was dispatched, -1 otherwise.
     *
     * @param  driverID  id of the driver
     * @param  clientID  id of the client
     * @param  when      date before which a dispatch may have occurred
     * @return           requestID for which the driver was dispatched, -1
     *                   otherwise
     */
    public int wasDispatched(int driverID, int clientID, Timestamp when) {
        
        try {
            
            // join dispatch and request
            query = "SELECT Request.request_id FROM Dispatch INNER JOIN "
            + "Request on Dispatch.request_id = Request.request_id WHERE "
            + "Dispatch.driver_id = ? and Request.client_id = ? and "
            + "Request.datetime <= ?;";
            
            ps = connection.prepareStatement(query);
            ps.setInt(1, driverID);
            ps.setInt(2, clientID);
            ps.setTimestamp(3, when);
            result = ps.executeQuery();
            
            
            if (result.next()) {
                
                return result.getInt("request_id");
                
            } else {
                
                // driver was not dispatched for this client
                return -1;
                
            }
            
        } catch (SQLException se) {
            se.printStackTrace();
            return -1;
        }
        
    }
    
    /**
     * Returns true if a pickup was recorded for a particular driver, client
     * pair, false otherwise
     *
     * @param  driverID  id of the driver
     * @param  clientID  id of the client
     * @param  when      client's pickup time
     * @return           true if a pickup was recorded for a particular driver,
     *                   client pair at time when, false otherwise
     */
    public boolean pickupRecorded(int driverID, int clientID, Timestamp when) {
        
        try {
            
            query = "SELECT pick.request_id, client_id, driver_id, pick.datetime "
            + "FROM (pickup pick JOIN dispatch dis ON pick.request_id "
            + "= dis.request_id) JOIN request req ON pick.request_id "
            + "= req.request_id WHERE dis.driver_id = ? AND "
            + "req.client_id = ? AND pick.datetime = ?;";
            
            ps = connection.prepareStatement(query);
            ps.setInt(1, driverID);
            ps.setInt(2, clientID);
            ps.setTimestamp(3, when);
            result = ps.executeQuery();
            
            return result.next();
            
        } catch (SQLException se) {
            
            query = "SELECT Request.request_id FROM "
            + "Dispatch INNER JOIN Request on Dispatch.request_id = "
            + "Request.request_id WHERE Dispatch.driver_id = ? and "
            + "Request.client_id = ? and Request.datetime <= ?;";
            se.printStackTrace();
            
            return false;
        }
    }
    
    /**
     * Records the fact that a driver has picked up a client.
     *
     * If the driver was dispatched to pick up the client and the corresponding
     * pick-up has not been recorded, records it by adding a row to the
     * Pickup table, and returns true.  Otherwise, returns false.
     *
     * @param  driverID  id of the driver
     * @param  clientID  id of the client
     * @param  when      the date and time when the pick-up occurred
     * @return           true if the operation was successful, false otherwise
     */
    public boolean picked_up(int driverID, int clientID, Timestamp when) {
        
        try {
            
            if (this.pickupRecorded(driverID, clientID, when)) {
                
                // nothing to do
                return false;
                
            } else {
                
                // check if driver was dispatched for the client
                int requestID = wasDispatched(driverID, clientID, when);
                
                if (requestID == -1) {
                    
                    // no such request ID exists
                    return false;
                    
                } else {
                    
                    query = "INSERT INTO Pickup VALUES (?, ?);";
                    
                    ps = connection.prepareStatement(query);
                    ps.setInt(1, requestID);
                    ps.setTimestamp(2, when);
                    
                    ps.execute();
                    
                    return true;
                }
            }
            
        } catch (SQLException se) {
            se.printStackTrace();
            return false;
        }
        
    }
    
    /* ===================== Dispatcher-related methods ===================== */
    
    
    /**
     * Finds clients who have requested a ride in the area but have not yet
     * been picked up.
     */
    public void findClients(PGpoint NW, PGpoint SE) {
        
        try {
            
            // pending requests
            query = "CREATE VIEW pending AS SELECT * FROM (SELECT request_id "
            + "FROM Request) req EXCEPT (SELECT request_id FROM Pickup);";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            // find clients and their location
            query = "CREATE TABLE clients_location as SELECT req.client_id, "
            + "req.request_id, location FROM (pending NATURAL JOIN Request) "
            + "req INNER JOIN Place ON req.source = Place.name;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            // select only clients within bounded region
            query = "CREATE TABLE area_clients AS SELECT * FROM "
            + "clients_location WHERE location[0] >= ? AND location[0] <= ? "
            + "AND location[1] >= ? AND location[1] <= ?;";
            ps = connection.prepareStatement(query);
            
            // CREATE VIEW area_clients as
            ps.setDouble(1, NW.x);
            ps.setDouble(2, SE.x);
            ps.setDouble(3, SE.y);
            ps.setDouble(4, NW.y);
            
            ps.execute();
            
        } catch (SQLException se) {
            se.printStackTrace();
        }
        
    }
    
    public void getDrivers(PGpoint NW, PGpoint SE) {
        
        try {
            
            query = "CREATE TABLE area_drivers AS SELECT a1.driver_id, "
            + "location FROM Available a1 INNER JOIN Dispatch d1 "
            + "ON a1.driver_id = d1.driver_id WHERE location[0] >= ? "
            + "AND location[0] <= ? AND location[1] >= ? AND "
            + "location[1] <= ? AND NOT EXISTS ( SELECT * FROM "
            + "Dispatch d2 WHERE a1.driver_id = d2.driver_id AND "
            + "a1.datetime < d2.datetime );";
            
            ps = connection.prepareStatement(query);
            
            ps.setDouble(1, NW.x);
            ps.setDouble(2, SE.x);
            ps.setDouble(3, SE.y);
            ps.setDouble(4, NW.y);
            
            ps.execute();
            
        } catch (SQLException se) {
            se.printStackTrace();
        }
        
    }
    
    /**
     * Dispatches drivers to the clients who've requested rides in the area
     * bounded by NW and SE.
     *
     * For all clients who have requested rides in this area (i.e., whose
     * request has a source location in this area), dispatches drivers to them
     * one at a time, from the client with the highest total billings down
     * to the client with the lowest total billings, or until there are no
     * more drivers available.
     *
     * Only drivers who (a) have declared that they are available and have
     * not since then been dispatched, and (b) whose location is in the area
     * bounded by NW and SE, are dispatched.  If there are several to choose
     * from, the one closest to the client's source location is chosen.
     * In the case of ties, any one of the tied drivers may be dispatched.
     *
     * Area boundaries are inclusive.  For example, the point (4.0, 10.0)
     * is considered within the area defined by
     *         NW = (1.0, 10.0) and SE = (25.0, 2.0)
     * even though it is right at the upper boundary of the area.
     *
     * Dispatching a driver is accomplished by adding a row to the
     * Dispatch table.  All dispatching that results from a call to this
     * method is recorded to have happened at the same time, which is
     * passed through parameter 'when'.
     *
     * @param  NW    x, y coordinates in the northwest corner of this area.
     * @param  SE    x, y coordinates in the southeast corner of this area.
     * @param  when  the date and time when the dispatching occurred
     */
    public void dispatch(PGpoint NW, PGpoint SE, Timestamp when) {
        
        // find all clients who have requested a ride in the area
        this.findClients(NW, SE);
        
        
        // find available drivers in area
        this.getDrivers(NW, SE);
        
        
        // rank clients by total billings
        ArrayList<Integer> clients = new ArrayList<Integer>();
        Integer clientID;
        
        try {
            
            query = "SELECT area_clients.client_id FROM Request "
            + "INNER JOIN Billed ON Request.request_id = "
            + "Billed.request_id RIGHT OUTER JOIN area_clients "
            + "ON Request.client_id = area_clients.client_id "
            + "GROUP BY area_clients.client_id ORDER BY "
            + "sum(amount) DESC;";
            
            ps = connection.prepareStatement(query);
            result = ps.executeQuery();
            
            while (result.next()) {
                
                // each clientID is added for processing
                clientID = result.getInt("client_id");
                clients.add(clientID);
                
            }
            
        } catch (SQLException se) {
            se.printStackTrace();
        }
        
        // dispatch closest available driver to clients in order of billings
        if (clients.size() > 0) {
            
            try {
                
                // all client as their distances to each driver
                query = "CREATE VIEW distances AS SELECT area_clients.client_id,"
                + " area_clients.request_id, area_drivers.driver_id, "
                + "area_drivers.location AS car_location, area_clients.location "
                + "<@> area_drivers.location AS distance FROM area_clients, "
                + "area_drivers;";
                ps = connection.prepareStatement(query);
                ps.execute();
                
                Integer curClient;
                
                // array clients is already sorted by clients' total billings
                for (int i = 0; i < clients.size(); i++) {
                    
                    curClient = clients.get(i);
                    
                    // clients and drivers who are available
                    query = "CREATE TABLE current_client AS SELECT * FROM "
                    + "distances d WHERE d.client_id = ? AND NOT EXISTS ( "
                    + "SELECT * FROM Dispatch dp WHERE dp.request_id = "
                    + "d.request_id AND dp.driver_id = d.driver_id);";
                    ps = connection.prepareStatement(query);
                    ps.setInt(1, curClient);
                    ps.execute();
                    
                    // find the shortest distance from any driver to the client
                    query = "CREATE VIEW minimum AS SELECT min(distance) "
                    + "as min FROM current_client;";
                    ps = connection.prepareStatement(query);
                    ps.execute();
                    
                    // find closest driver to this client
                    query = "SELECT request_id, driver_id, car_location FROM "
                    + " distances, minimum WHERE distance = min;";
                    ps = connection.prepareStatement(query);
                    result = ps.executeQuery();
                    
                    result.next();
                    Integer driverID = result.getInt("driver_id");
                    Integer requestID = result.getInt("request_id");
                    PGpoint carLocation =
                    (PGpoint) result.getObject("car_location");
                    
                    // dispatch driver for this client
                    query = "INSERT INTO Dispatch VALUES (?, ?, ?, ?);";
                    ps = connection.prepareStatement(query);
                    ps.setInt(1, requestID);
                    ps.setInt(2, driverID);
                    ps.setObject(3, carLocation);
                    ps.setTimestamp(4, when);
                    ps.execute();
                    
                    // drop views current client and minimum
                    query = "DROP TABLE IF EXISTS current_client CASCADE;";
                    ps = connection.prepareStatement(query);
                    ps.execute();
                    
                    query = "DROP VIEW IF EXISTS minimum CASCADE;";
                    ps = connection.prepareStatement(query);
                    ps.execute();
                    
                }
                
                // drop all views created during call to dispatch
                
                query = "DROP VIEW IF EXISTS pending CASCADE;";
                ps = connection.prepareStatement(query);
                ps.execute();
                
                query = "DROP VIEW IF EXISTS distances CASCADE;";
                ps = connection.prepareStatement(query);
                ps.execute();
                
                query = "DROP TABLE IF EXISTS clients_location CASCADE;";
                ps = connection.prepareStatement(query);
                ps.execute();
                
                query = "DROP TABLE IF EXISTS area_clients CASCADE;";
                ps = connection.prepareStatement(query);
                ps.execute();
                
                query = "DROP TABLE IF EXISTS area_drivers CASCADE;";
                ps = connection.prepareStatement(query);
                ps.execute();
                
            } catch (SQLException se) {
                se.printStackTrace();
            }
            
        }
        
    }
    
    public void dropViews() {
        
        try{
            
            query = "DROP TABLE IF EXISTS current_client CASCADE;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            query = "DROP VIEW IF EXISTS pending CASCADE;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            query = "DROP VIEW IF EXISTS distances CASCADE;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            query = "DROP TABLE IF EXISTS clients_location CASCADE;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            query = "DROP TABLE IF EXISTS area_clients CASCADE;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
            query = "DROP TABLE IF EXISTS area_drivers CASCADE;";
            ps = connection.prepareStatement(query);
            ps.execute();
            
        } catch (SQLException se) {
            se.printStackTrace();
        }
        
    }
    
}