import java.io.*;
import java.sql.*;
import java.util.Properties;
import org.apache.log4j.Logger;

public class BatchResetCon
{

    private static Connection conn;
    private String server;
    private String host;
    private String port;
    private String sName;
    private String user;
    private String pass;
    private CallableStatement cs;
    private static Logger log;
    private static final String PROC_REPUSH = "{call rpt_usr.proj_batch_reset_get_repush(?,?)}";
    private static final String GET_BATCH = "select batch_id from rpt_usr.PLDT_BATCH_RESET_BAT where batch_status = 'PENDING' order by batch_id";
    private static final String UPD_BATCH = "update rpt_usr.PLDT_BATCH_RESET_BAT set batch_status = ?, cnt_fail=?, cnt=?, lu_dt=sysdate where batch_id = ?";

    public BatchResetCon()
    {
        log = Logger.getLogger(getClass().getSimpleName());
    }

    public void connect()
    {
        try
        {
            log.debug("Connect to KenanDB");
            String driver = (new StringBuilder("jdbc:oracle:thin:@")).append(host).append(":").append(port).append(":").append(sName).toString();
            conn = DriverManager.getConnection(driver, user, pass);
            conn.setAutoCommit(false);
            log.debug((new StringBuilder("Connected to ")).append(server).toString());
        }
        catch(SQLException e)
        {
            log.error(e.fillInStackTrace());
            for(int i = 0; i < e.getStackTrace().length; i++)
            {
                log.error(e.getStackTrace()[i]);
            }

            e.printStackTrace();
        }
    }

    public void connect(String server)
    {
        try
        {
            log.debug((new StringBuilder("Connecting to ")).append(server).toString());
            Properties dbProperties = new Properties();
            try
            {
                dbProperties.load(new FileInputStream(new File((new StringBuilder(String.valueOf(server))).append(".properties").toString())));
            }
            catch(FileNotFoundException e)
            {
                e.printStackTrace();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            host = dbProperties.getProperty("KHOST");
            port = dbProperties.getProperty("KPORT");
            sName = dbProperties.getProperty("KSNAME");
            user = dbProperties.getProperty("UNAME");
            pass = dbProperties.getProperty("PWD");
            String driver = (new StringBuilder("jdbc:oracle:thin:@")).append(host).append(":").append(port).append(":").append(sName).toString();
            conn = DriverManager.getConnection(driver, user, pass);
            conn.setAutoCommit(false);
            log.debug((new StringBuilder("Connected to ")).append(server).toString());
        }
        catch(SQLException e)
        {
            log.error(e.fillInStackTrace());
            for(int i = 0; i < e.getStackTrace().length; i++)
            {
                log.error(e.getStackTrace()[i]);
            }

            e.printStackTrace();
        }
    }

    public Boolean callProc(String proc, String server)
    {
        try {
    	log.debug("entered");
        connect(server);
        String simpleProc = (new StringBuilder("{ call ")).append(proc).append(" }").toString();
        log.debug((new StringBuilder("proc call: ")).append(simpleProc).toString());
        cs = conn.prepareCall(simpleProc);
        cs.execute();
        conn.close();
        log.debug("done");
        return Boolean.valueOf(true);
        }
        catch(SQLException e)
        {
            log.error(e.getMessage().replaceAll("(?:\\n|\\r)", " ").trim());
            return Boolean.valueOf(false);
        }
        
    }

    public ResultSet getRepush(int BatchId)
    {
        ResultSet rs = null;
        log.debug("entered");
        try
        {
            cs = conn.prepareCall(PROC_REPUSH);
            cs.setInt(1, BatchId);
            cs.registerOutParameter(2, -10);
            cs.execute();
            rs = (ResultSet)cs.getObject(2);
            log.debug("done");
        }
        catch(SQLException e)
        {
            log.error(e.getMessage().replaceAll("(?:\\n|\\r)", " ").trim());
        }
        return rs;
    }

    public ResultSet getBatch()
    {
        Statement stmt = null;
        ResultSet rs = null;
        log.debug("enter getBatch");
        try
        {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(GET_BATCH);
        }
        catch(SQLException e)
        {
            log.error(e.getMessage().replaceAll("(?:\\n|\\r)", " ").trim());
        }
        log.debug("exit getBatch");
        return rs;
    }

    public void updBatch(String StatName, int CntFail, int Cnt, int BatchId)
    {
        PreparedStatement pstmt = null;
        log.debug("enter updBatch");
        try
        {
            pstmt = conn.prepareStatement(UPD_BATCH);
            pstmt.setString(1, StatName);
            pstmt.setInt(2, CntFail);
            pstmt.setInt(3, Cnt);
            pstmt.setInt(4, BatchId);
            
            pstmt.executeUpdate();
            conn.commit();
        }
        catch(SQLException e)
        {
            log.error(e.getMessage().replaceAll("(?:\\n|\\r)", " ").trim());
        }
        log.debug("exit updBatch");
    }

    public void disconnect()
    {
        try
        {
            cs.close();
            conn.close();
            log.debug("Disconnected");
        }
        catch(SQLException e)
        {
            log.error(e.fillInStackTrace());
            for(int i = 0; i < e.getStackTrace().length; i++)
            {
                log.error(e.getStackTrace()[i]);
            }

            e.printStackTrace();
        }
    }

    static 
    {
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException e)
        {
            log.error(e.fillInStackTrace());
            for(int i = 0; i < e.getStackTrace().length; i++)
            {
                log.error(e.getStackTrace()[i]);
            }

            e.printStackTrace();
        }
    }
}
