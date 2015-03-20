using System;
using System.Data;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using MySql.Data.MySqlClient;
using log4net;

/// <summary>
/// Summary description for DBConnect
/// </summary>
public class DBConnect
{
    private MySqlConnection connection;
    private Boolean pool = false;
    String path;
    private static readonly ILog kplog = LogManager.GetLogger(typeof(DBConnect));
    //Constructor
    public DBConnect(String Serv, String DB, String UID, String Password, String pooling, Int32 maxcon, Int32 mincon, Int32 tout)
    {
        Initialize(Serv, DB, UID, Password, pooling, maxcon, mincon, tout);
    }

    //Initialize values
    private void Initialize(String Serv, String DB, String UID, String Password, String pooling, Int32 maxcon, Int32 mincon, Int32 tout)
    {
        try
        {
            if (pooling.Equals("1"))
            {
                pool = true;
            }

            string myconstring = "server = " + Serv + "; database = " + DB + "; uid = " + UID + ";password= " + Password + "; pooling=" + pool + ";min pool size=" + mincon + ";max pool size=" + maxcon + ";connection lifetime=0; Command Timeout=28800; connection timeout=" + tout + ";Allow Zero Datetime=true";
            connection = new MySqlConnection(myconstring);
        }catch(Exception ex){
            kplog.Fatal("Unable to connect",ex);
            throw new Exception(ex.Message);
        }
     
    }

    public String Path
    {
        get { return path; }
        set { path = value; }
    }
    //open connection to database
    public bool OpenConnection()
    {
        try
        {
            connection.Open();
            return true;
        }
        catch (MySqlException)
        {
            //When handling errors, you can your application's response based 
            //on the error number.
            //The two most common error numbers when connecting are as follows:
            //0: Cannot connect to server.
            //1045: Invalid user name and/or password.
            return false;
        }
    }

    //Close connection
    public bool CloseConnection()
    {
        try
        {
            connection.Close();
            return true;
        }
        catch (MySqlException)
        {
            return false;
        }
    }

    //Insert statement
    public void Insert()
    {
    }

    //Update statement
    public void Update()
    {
    }

    //Delete statement
    public void Delete()
    {
    }

    public MySqlConnection getConnection()
    {
        return connection;
    }

    public void dispose()
    {
        connection.Dispose();
    }

    ////Select statement
    //public List<string>[] Select()
    //{
    //}

    ////Count statement
    //public int Count()
    //{
    //}

    //Backup
    public void Backup()
    {
    }

    //Restore
    public void Restore()
    {
    }
}
