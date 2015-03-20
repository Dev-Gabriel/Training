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

/// <summary>
/// Summary description for CustomerDBCon
/// </summary>
public class CustomerDBCon
{
    private MySqlConnection connection;
    private String Serv = "";
    private String DB = "";
    private String UID = "";
    private String Password = "";
    private Boolean pool = false;
    private int maxcon = 100;
    private int mincon = 0;
    private int tout = 30;
	public CustomerDBCon()
	{
        Initialize();
	}

    private void Initialize()
    {

        //String path = HttpContext.Current.Server.MapPath("C:\\config\\boskpws.ini");
        IniFile ini = new IniFile("C:\\kpconfig\\globalConf.ini");

        Serv = ini.IniReadValue("DBConfig Customer", "Server");
        DB = ini.IniReadValue("DBConfig Customer", "Database"); ;
        UID = ini.IniReadValue("DBConfig Customer", "UID"); ;
        Password = ini.IniReadValue("DBConfig Customer", "Password"); ;
        String a = ini.IniReadValue("DBConfig Customer", "Pool"); ;
        if (a.Equals("1"))
        {
            pool = true;
        }
        maxcon = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "MaxCon"));
        mincon = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "MinCon"));
        tout = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "Tout"));
        String MyConString = "SERVER = " + Serv + "; DATABASE = " + DB + "; UID = " + UID + ";PASSWORD= " + Password + "; Pooling=" + pool + ";Min Pool Size=" + mincon + ";Max Pool Size=" + maxcon + ";Connection Lifetime=0; Command Timeout=28800; Connection Timeout=" + tout + "";
        //var connectionString = ConfigurationManager.ConnectionStrings["kpcustomers"];
        //string cons = connectionString.ConnectionString;
        connection = new MySqlConnection(MyConString);
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
