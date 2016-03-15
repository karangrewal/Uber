import java.sql.*;
// You should use this class so that you can represent SQL points as
// Java PGpoint objects.
import org.postgresql.geometric.PGpoint;

// If you are looking for Java data structures, these are highly useful.
// However, you can write the solution without them.  And remember
// that part of your mark is for doing as much in SQL (not Java) as you can.
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class App {

   // A connection to the database
   Connection connection;
   
   // 
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
           query = "set search_path to uber;";
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
           
           /* CODE FOR INSERTING DRIVER INTO Driver TABLE
           
           Date date = new Date(2012, 12, 21);
           query = "INSERT INTO Driver VALUES (69, 'horton', 'diane', ?, '40 St George Street', 'impala', true);";
           ps = connection.prepareStatement(query);
           ps.setDate(1, date);
           ps.execute();
           
           */
           
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
    * @return           true if a pickup was recorded for a particular driver, client
    *                   pair at time when, false otherwise
    */
   public boolean pickupRecorded(int driverID, int clientID, Timestamp when) {
   
       try {
       
           query = "SELECT pick.request_id, client_id, driver_id, pick.datetime"
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
     *
     */
   public void findClients(PGpoint NW, PGpoint SE, Timestamp when) {
       
       try {
       
           // find pending requests in area
           query = "CREATE VIEW area_clients as SELECT Request.client_id, Request.request_id, location "
               + "FROM (((SELECT request_id FROM Request) EXCEPT " 
               + "(SELECT request_id FROM Pickup)) NATURAL JOIN Request) INNER "
               + "JOIN Place ON Request.source = Place.name WHERE location[0] >= "
               + "? AND location[0] <= ? and location[0] >= ? and location[0] <= "
               + "?;";
           
           ps = connection.prepareStatement(query);
           ps.setDouble(1, NW.x);
           ps.setDouble(2, SE.x);
           ps.setDouble(3, SE.y);
           ps.setDouble(4, NW.y);
           ps.setTimestamp(2, when);
            
           ps.execute();
           
       } catch (SQLException se) {
           se.printStackTrace();
       }
       
   }
   
   public void getDrivers(PGpoint NW, PGpoint SE) {
   
       try {
       
           query = "CREATE VIEW area_drivers AS SELECT Available.driver_id, location FROM Available a1 INNER JOIN Dispatch d1 ON a1.driver_id = d1.driver_id WHERE location.point[0] >= ? AND location.point[0] <= ? AND location.point[1] >= ? AND location.point[1] <= ? AND NOT EXISTS ( SELECT * FROM Dispatch d2 WHERE a1.driver_id = d2.driver_id AND a1.datetime < d2.datetime );";
       
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
      this.findClients(NW, SE, when);
      
      // find available drivers in area
      this.getDrivers(NW, SE);
      
      // rank clients by total billings
      ArrayList<Integer> clients = new ArrayList<Integer>();
      Integer clientID;
      
      try {
      
          query = "SELECT area_clients.client_id FROM Request INNER JOIN Billed ON Request.request_id = Billed.request_id INNER JOIN area_clients ON Request.client_id = area_clients.client_id GROUP BY area_clients.client_id ORDER BY sum(amount) DESC;";
          
          ps = connection.prepareStatement(query);
          result = ps.executeQuery();
          
          while (result.next()) {
              
              clientID = result.getInt("client_id");
              clients.add(clientID);
          
          }
      
      } catch (SQLException se) {
          se.printStackTrace();
      }
      
      // dispatch closest available driver to clients in order of billings
      if (clients.size() > 0) {
          
          try {
              query = "CREATE VIEW distances AS  SELECT area_clients.client_id, area_clients.request_id, area_drivers.driver_id, area_drivers.location AS car_location, area_clients.location <@> area_drivers.location AS distance FROM area_clients, area_drivers;";
              ps = connection.prepareStatement(query);
              ps.execute();
          
              Integer curClient;
          
              // array clients is already sorted by clients' total billings
              for (int i = 0; i < clients.size(); i++) {
          
                  curClient = clients.get(i);
              
                  /*
                  1) create view for curClient with all drivers s.t. driver has not already been dispatched for a request id in the distances table
                  2) find min and dispatch driver -- update tables / views and shit
                  */
              
                  // clients and drivers who are available
                  query = "CREATE VIEW current_client AS SELECT * FROM distances d WHERE distances.client_id = ? AND NOT EXISTS ( SELECT * FROM Dispatch dp WHERE dp.request_id = d.request_id AND dp.driver_id = d.driver_id);";
                  ps = connection.prepareStatement(query);
                  ps.setInt(1, curClient);
                  ps.execute();
              
                  // find closest driver and dispatch
                  query = "CREATE VIEW minimum AS SELECT min(distance) as min FROM current_client;";
                  ps = connection.prepareStatement(query);
                  ps.execute();
              
                  query = "SELECT request_id, driver_id, car_location FROM distances, minimum WHERE distance = min;";
                  ps = connection.prepareStatement(query);
                  result = ps.executeQuery();
              
                  if (result.next()) {
                  
                      Integer driverID = result.getInt("driver_id");
                      Integer requestID = result.getInt("request_id");
                      PGpoint carLocation = (PGpoint) result.getObject("car_location");
                  
                      query = "INSERT INTO Dispatch VALUES (?, ?, ?, ?);";
                      ps = connection.prepareStatement(query);
                      ps.setInt(1, requestID);
                      ps.setInt(2, driverID);
                      ps.setObject(3, carLocation); 
                      ps.setTimestamp(4, when);
                  
                  }
              }
          
          } catch (SQLException se) {
              se.printStackTrace();
          }
      
      }
      
   }

   /** main method

   public static void main(String[] args) {
      // You can put testing code in here. It will not affect our autotester.
      System.out.println("Boo!");
      
      // testing
      
      App a2;
      
      try {
          a2 = new App();
          boolean f = a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-g4sharma", "g4sharma", "");
      
          if (f) {
          
              //PGpoint p1 = new PGpoint(1.0, 10.2);
              //PGpoint p2 = new PGpoint(9.0, 2.0);
              Timestamp t = new Timestamp(2016, 3, 15, 11, 0, 0, 0);
              
              int r = a2.wasDispatched(373, 343, t);
              System.out.println(r);
              
              
          
          
          } else {
      
              System.out.println("Didn't connect");
          }
      
      
          a2.disconnectDB();
          
          
      } catch (SQLException se) {
          System.out.println("can't initialize a2");
      }
   }
   */

}