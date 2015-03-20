using System;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Web.Services.Protocols;
using System.Xml.Linq;
using MySql.Data.MySqlClient;
using System.Text;
using System.Collections.Generic;
using System.Collections;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Data;
using System.Security.Cryptography;
using log4net;
using log4net.Config;

[WebService(Namespace = "http://localhost/", Description = "MLKP Web Service", Name = "MLhuillier")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
// To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
// [System.Web.Script.Services.ScriptService]
public class Service : System.Web.Services.WebService
{
    private MySqlCommand command;
    private MySqlCommand custcommand;
    //private MySqlDataReader Reader;
    private DBConnect dbconGlobal;
    private DBConnect custconGlobal;
    private DBConnect dbconDomestic;
    private DBConnect custconDomestic;
    private MySqlTransaction trans = null;
    private MySqlTransaction custtrans = null;
    private Int32 isUse365Global;
    private Int32 isUseKYCGlobal;
    private Int32 isUse365Domestic;
    private Int32 isUseKYCDomestic;
    private DateTime dt;
    private const String loginuser = "boswebserviceusr";
    private const String loginpass = "boyursa805";
    private String pathGlobal;
    private String pathDomestic;
    private const String currentVersion = "7";
    private DBConnect CMMSConnectGlobal;
    private DBConnect CMMSConnectDomestic;
    private static readonly ILog kplog = LogManager.GetLogger(typeof(Service));

    public Service()
    {
        try
        {
            pathGlobal = "C:\\kpconfig\\globalConf.ini";
            pathDomestic = "C:\\kpconfig\\boskpConf.ini";

            IniFile iniGlobal = new IniFile(pathGlobal);
            IniFile iniDomestic = new IniFile(pathDomestic);

            isUse365Global = Convert.ToInt32(iniGlobal.IniReadValue("Use 365", "use365"));
            isUseKYCGlobal = Convert.ToInt32(iniGlobal.IniReadValue("Use KYC", "usekyc"));

            isUse365Domestic = Convert.ToInt32(iniDomestic.IniReadValue("Use 365", "use365"));
            isUseKYCDomestic = Convert.ToInt32(iniDomestic.IniReadValue("Use KYC", "usekyc"));

            ConnectGlobal();
            ConnectDomestic();

            log4net.Config.XmlConfigurator.Configure();
            //Logging.SetParameter("bcode", "");
            //Logging.SetParameter("zcode", "");
            //kplog.Info("boskpws constructor accessed");

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: message: "+ex.Message, ex);
            throw new Exception(ex.Message);
        }
    }

    [WebMethod]
    public sysAdTester sysAdTestMe()
    {
        try
        {
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    custconGlobal.getConnection().Open();
                    custconGlobal.getConnection().Close();
                    con.Open();
                    con.Close();
                    kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", numberOfDBs: 2, DBConnection: Connected, ConnectionStringTransaction" + con.ConnectionString + ", ConnectionStringCustomer: " + custconGlobal.getConnection().ConnectionString + ", Version: " + currentVersion);
                    return new sysAdTester { respcode = 1, message = getRespMessage(1), numberOfDBs = 2, DBConnection = "Connected", ConnectionStringTransaction = con.ConnectionString, ConnectionStringCustomer = custconGlobal.getConnection().ConnectionString, Version = currentVersion };
                }
                catch (MySqlException mex)
                {
                    con.Close();
                    kplog.Fatal("FAILED:: respcode: 0 , message : " + mex.ToString() + ", DBConnection: Not Connected , ConnectionStringTransaction: " + con.ConnectionString);
                    return new sysAdTester { respcode = 0, message = getRespMessage(0), DBConnection = "Not Connected", ConnectionStringTransaction = con.ConnectionString, errorDetail = mex.ToString() };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0, message : " + getRespMessage(0) + ", DBConnection: Not Connected, ErrorDetail: " + ex.ToString());
            return new sysAdTester { respcode = 0, message = getRespMessage(0), DBConnection = "Not Connected", errorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public AllowedIDPerZone getIDsGlobal(String Username, String Password, Int32 zonecode)
    {
        kplog.Info("Username: "+Username+", Password: "+Password+", ZoneCode: "+zonecode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new AllowedIDPerZone { respcode = 7, message = getRespMessage(7) };
        }

        try
        {
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    con.Open();
                    List<string> listofids = new List<string>();
                    int x = 0;
                    using (command = con.CreateCommand())
                    {
                        command.CommandText = "select idtype from kpformsglobal.sysallowedidtype where zonecode = @zcode";
                        command.Parameters.AddWithValue("zcode", zonecode);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    listofids.Add(reader[x].ToString());
                                    x = x++;
                                }
                                reader.Close();
                                con.Close();
                                kplog.Info("SUCCESS: respcode: 1, message : " + getRespMessage(1) + ", IDs: " + listofids);
                                return new AllowedIDPerZone { respcode = 1, message = getRespMessage(1), IDs = listofids };

                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 0 , message  : No IDs found in your zone.");
                                reader.Close();
                                con.Close();
                                return new AllowedIDPerZone { respcode = 0, message = "No IDs found in your zone." };

                            }
                        }
                    }
                }
                catch (MySqlException mex)
                {

                    kplog.Fatal("FAILED:: respcode: 0 , message : " + mex.ToString());
                    con.Close();
                    return new AllowedIDPerZone { respcode = 0, message = mex.Message, ErrorDetail = mex.ToString() };
                }
            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            return new AllowedIDPerZone { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }

    }

    [WebMethod]
    public State getStateGlobal(String Username, String Password)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password );

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new State { respcode = 7, message = getRespMessage(7) };
        }

        try
        {
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    con.Open();
                    List<string> listofstates = new List<string>();
                    int x = 0;
                    using (command = con.CreateCommand())
                    {
                        command.CommandText = "select state from kpformsglobal.sysstate where isactive='1'";

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    listofstates.Add(reader[x].ToString());
                                    x = x++;
                                }
                                reader.Close();
                                con.Close();
                                kplog.Info("SUCCES:: respcode: 1, message: " + getRespMessage(1) + ", States: " + listofstates);
                                return new State { respcode = 1, message = getRespMessage(1), States = listofstates };

                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 0, message : No State found or not State not active.");
                                reader.Close();
                                con.Close();
                                return new State { respcode = 0, message = "No State found or not State not active." };

                            }
                        }
                    }
                }
                catch (MySqlException mex)
                {

                    kplog.Fatal("FAILED:: respocde: 0 , message : " + mex.ToString());
                    con.Close();
                    return new State { respcode = 0, message = mex.Message, ErrorDetail = mex.ToString() };
                }
            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0, message : " + ex.ToString());
            return new State { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }

    }


    [WebMethod]
    public AllowedIDPerZone getIDsDomestic(String Username, String Password, Int32 zonecode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", ZoneCode: " + zonecode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new AllowedIDPerZone { respcode = 7, message = getRespMessage(7) };
        }

        try
        {
            using (MySqlConnection con = dbconDomestic.getConnection())
            {
                try
                {
                    con.Open();
                    List<string> listofids = new List<string>();
                    int x = 0;
                    using (command = con.CreateCommand())
                    {
                        command.CommandText = "select idtype from kpforms.sysallowedidtype where zonecode = @zcode";
                        command.Parameters.AddWithValue("zcode", zonecode);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    listofids.Add(reader[x].ToString());
                                    x = x++;
                                }
                                reader.Close();
                                con.Close();
                                kplog.Info("SUCCES:: respcode: 1, message : " + getRespMessage(1) + " , IDs : " + listofids);
                                return new AllowedIDPerZone { respcode = 1, message = getRespMessage(1), IDs = listofids };

                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 0 , message : No IDs found in your zone.");
                                reader.Close();
                                con.Close();
                                return new AllowedIDPerZone { respcode = 0, message = "No IDs found in your zone." };

                            }
                        }
                    }
                }
                catch (MySqlException mex)
                {

                    kplog.Fatal("FAILED:: respcode: 0 , message : " + mex.ToString());
                    con.Close();
                    return new AllowedIDPerZone { respcode = 0, message = mex.Message, ErrorDetail = mex.ToString() };
                }
            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode : 0 , message : " + ex.ToString());
            return new AllowedIDPerZone { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }

    }


    [WebMethod]
    public Int32 getKYCStatusGlobal(String Username, String Password, Double version, String stationcode)
    {

        kplog.Info("Username: " + Username + ", Password: " + Password + ", version: " + version + ", stationcode: " + stationcode);

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}

            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                throw new Exception("Invalid credentials");
            }
            return isUseKYCGlobal;
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    [WebMethod]
    public Int32 getKYCStatus(String Username, String Password, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", version: " + version + ", stationcode: " + stationcode);
        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}

            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                throw new Exception("Invalid credentials");
            }
            return isUseKYCDomestic;
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    //Domestic : 0 = standard, 1 = per branch, 2 = promo, 3 = cancel
    [WebMethod]
    public ChargeResponse getDomesticCharges(String Username, String Password, Int16 chargetype, String bcode, Int16 zcode, String promoname, Double version, String stationcode, Double amount)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", chargetype: " + chargetype + ", bcode: " + bcode + ", zcode: " + zcode + ", promoname: " + promoname + ", version: " + version + ", stationcode: " + stationcode + ", amount: " + amount);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        try
        {
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    con.Open();
                    Int32 xtype = 0;
                    Double minamt = 0;
                    string qrygetminamount = String.Empty;
                    string qrygetcharge = String.Empty;
                    using (command = con.CreateCommand())
                    {
                        if (chargetype == 0)
                        {
                            qrygetminamount = "select MIN(c.MinAmount) as minimum from kpformsglobal.charges c inner join kpformsglobal.headercharges hc on hc.currID = c.Type where hc.cEffective = 1";
                        }
                        else if (chargetype == 1)
                        {
                            qrygetminamount = "select MIN(c.MinAmount) as minimum from kpformsglobal.ratesperbranchcharges c inner join kpformsglobal.ratesperbranchheader hc on hc.currID = c.Type where hc.cEffective = 1";
                        }
                        else
                        {
                            qrygetminamount = "select MIN(c.MinAmount) as minimum from kpformsglobal.promoratescharges c inner join kpformsglobal.promoratesheader hc on hc.currID = c.Type where hc.cEffective = 1";
                        }
                        command.CommandText = qrygetminamount;
                        command.Parameters.Clear();
                        MySqlDataReader rdrminamt = command.ExecuteReader();
                        if (rdrminamt.Read())
                        {
                            xtype = 1;
                            minamt = Convert.ToDouble(rdrminamt["minimum"].ToString());
                        }
                        rdrminamt.Close();

                        if (xtype == 1)
                        {
                            if (chargetype == 0)
                            {
                                qrygetcharge = "select c.ChargeValue as charge from kpformsglobal.charges c inner join kpformsglobal.headercharges hc on hc.currID = c.Type where hc.cEffective = 1 and (@amount BETWEEN c.MinAmount and c.MaxAmount) and c.MinAmount >= @minamt";
                            }
                            else if (chargetype == 1)
                            {
                                qrygetcharge = "select c.ChargeValue as charge from kpformsglobal.ratesperbranchcharges c inner join kpformsglobal.ratesperbranchheader hc on hc.currID = c.Type where hc.cEffective = 1 and (@amount BETWEEN c.MinAmount and c.MaxAmount) and c.MinAmount >= @minamt";
                            }
                            else
                            {
                                qrygetcharge = "select c.ChargeValue as charge from kpformsglobal.promoratescharges c inner join kpformsglobal.promoratesheader hc on hc.currID = c.Type where hc.cEffective = 1 and (@amount BETWEEN c.MinAmount and c.MaxAmount) and c.MinAmount >= @minamt";
                            }
                        }
                        else
                        {
                            con.Close();
                            kplog.Error("FAILED:: respcode: 0 , message: No value for charge selected!");
                            return new ChargeResponse { respcode = 0, message = "No value for charge selected" };
                        }
                        command.CommandText = qrygetcharge;
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("amount", amount);
                        command.Parameters.AddWithValue("minamt", minamt);
                        MySqlDataReader rdrgetcharge = command.ExecuteReader();
                        if (rdrgetcharge.Read())
                        {
                            rdrgetcharge.Close();
                            con.Close();
                            kplog.Info("SUCCESS:: respcode:  1 , message: " + getRespMessage(1) + ", charge : " + Convert.ToDecimal(rdrgetcharge["charge"].ToString()));
                            return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = Convert.ToDecimal(rdrgetcharge["charge"].ToString()) };
                        }

                        rdrgetcharge.Close();
                        con.Close();
                        kplog.Error("FAILED:: respcode: 0 , message : No Charges Found! , charge: 0");
                        return new ChargeResponse { respcode = 0, message = "No Charges found", charge = 0 };
                    }
                }
                catch (MySqlException mex)
                {

                    kplog.Fatal("FAILED:: respcode: 0 , message: " + mex.ToString());
                    con.Close();
                    return new ChargeResponse { respcode = 0, message = mex.Message, ErrorDetail = mex.ToString() };
                }
            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message: " + ex.ToString());
            return new ChargeResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }
    }

    //Global : 0 = standard, 1 = per branch, 2 = promo, 3 = cancel
    [WebMethod]
    public ChargeResponse getGlobalCharges(String Username, String Password, Int16 chargetype, String bcode, Int16 zcode, String promoname, Double version, String stationcode, Double amount)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", chargetype: " + chargetype + ", bcode: " + bcode + ", zcode: " + zcode + ", promoname: " + promoname + ", version: " + version + ", stationcode: " + stationcode + ", amount: " + amount);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        try
        {
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    con.Open();
                    Int32 xtype = 0;
                    Double minamt = 0;
                    string qrygetminamount = String.Empty;
                    string qrygetcharge = String.Empty;
                    using (command = con.CreateCommand())
                    {
                        if (chargetype == 0)
                        {
                            qrygetminamount = "select MIN(c.MinAmount) as minimum from kpformsglobal.chargesG c inner join kpformsglobal.headerchargesG hc on hc.currID = c.Type where hc.cEffective = 1";
                        }
                        else if (chargetype == 1)
                        {
                            qrygetminamount = "select MIN(c.MinAmount) as minimum from kpformsglobal.ratesperbranchchargesG c inner join kpformsglobal.ratesperbranchheaderG hc on hc.currID = c.Type where hc.cEffective = 1";
                        }
                        else
                        {
                            qrygetminamount = "select MIN(c.MinAmount) as minimum from kpformsglobal.promorateschargesG c inner join kpformsglobal.promoratesheaderG hc on hc.currID = c.Type where hc.cEffective = 1";
                        }
                        command.CommandText = qrygetminamount;
                        command.Parameters.Clear();
                        MySqlDataReader rdrminamt = command.ExecuteReader();
                        if (rdrminamt.Read())
                        {
                            xtype = 1;
                            minamt = Convert.ToDouble(rdrminamt["minimum"].ToString());
                        }
                        rdrminamt.Close();

                        if (xtype == 1)
                        {
                            if (chargetype == 0)
                            {
                                qrygetcharge = "select c.ChargeValue as charge from kpformsglobal.chargesG c inner join kpformsglobal.headerchargesG hc on hc.currID = c.Type where hc.cEffective = 1 and (@amount BETWEEN c.MinAmount and c.MaxAmount) and c.MinAmount >= @minamt";
                            }
                            else if (chargetype == 1)
                            {
                                qrygetcharge = "select c.ChargeValue as charge from kpformsglobal.ratesperbranchchargesG c inner join kpformsglobal.ratesperbranchheaderG hc on hc.currID = c.Type where hc.cEffective = 1 and (@amount BETWEEN c.MinAmount and c.MaxAmount) and c.MinAmount >= @minamt";
                            }
                            else
                            {
                                qrygetcharge = "select c.ChargeValue as charge from kpformsglobal.promorateschargesG c inner join kpformsglobal.promoratesheaderG hc on hc.currID = c.Type where hc.cEffective = 1 and (@amount BETWEEN c.MinAmount and c.MaxAmount) and c.MinAmount >= @minamt";
                            }
                        }
                        else
                        {
                            con.Close();
                            kplog.Error("FAILED:: respcode: 0 , message : No value for charge Selected!");
                            return new ChargeResponse { respcode = 0, message = "No value for charge selected" };
                        }
                        command.CommandText = qrygetcharge;
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("amount", amount);
                        command.Parameters.AddWithValue("minamt", minamt);
                        MySqlDataReader rdrgetcharge = command.ExecuteReader();
                        if (rdrgetcharge.Read())
                        {
                            rdrgetcharge.Close();
                            con.Close();
                            kplog.Info("SUCCESS:: respcode: 1 , message : " + getRespMessage(1) + ", Charge: " + Convert.ToDecimal(rdrgetcharge["charge"].ToString()));
                            return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = Convert.ToDecimal(rdrgetcharge["charge"].ToString()) };
                        }

                        rdrgetcharge.Close();
                        con.Close();
                        kplog.Error("FAILED:: respcode: 0 , message : No Charges Found!, charge : 0");
                        return new ChargeResponse { respcode = 0, message = "No Charges found", charge = 0 };
                    }
                }
                catch (MySqlException mex)
                {

                    kplog.Fatal("FAILED:: respcode: 0 , message : " + mex.ToString());
                    con.Close();
                    return new ChargeResponse { respcode = 0, message = mex.Message, ErrorDetail = mex.ToString() };
                }
            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            return new ChargeResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }
    }

    //0 = standard, 1 = per branch, 2 = promo
    [WebMethod]
    public Decimal getMaxAmountGlobalUS(String Username, String Password, Int16 chargetype, String bcode, Int16 zcode, String promoname, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", chargetype: " + chargetype + ", bcode: " + bcode + ", zcode: " + zcode + ", promoname: " + promoname + ", version: " + version + ", stationcode: " + stationcode);

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    con.Open();
                    if (chargetype == 0)
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpformsglobal.chargesG c inner join kpformsglobal.headerchargesG hc on hc.currID = c.Type where hc.cEffective = 1;";
                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                    else if (chargetype == 1)
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpformsglobal.ratesperbranchchargesG c inner join kpformsglobal.ratesperbranchheaderG hc on hc.currID = c.Type where hc.cEffective = 1 and hc.branchcode = @bcode and hc.zonecode = @zcode;";
                            command.Parameters.AddWithValue("bcode", bcode);
                            command.Parameters.AddWithValue("zcode", zcode);
                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                    else
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpformsglobal.promorateschargesG c inner join kpformsglobal.promoratesheaderG hc on hc.currID = c.Type where hc.promoname = @promoname;";
                            command.Parameters.AddWithValue("promoname", promoname);

                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                }
                catch (MySqlException mex)
                {
                    kplog.Fatal("FAILED:: respcode: message : " + mex.ToString());
                    throw new Exception(mex.ToString());
                    //return 0;
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            throw new Exception(ex.ToString());
            //return 0;
        }
    }

    //0 = standard, 1 = per branch, 2 = promo
    [WebMethod]
    public Decimal getMaxAmountGlobal(String Username, String Password, Int16 chargetype, String bcode, Int16 zcode, String promoname, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", chargetype: " + chargetype + ", bcode: " + bcode + ", zcode: " + zcode + ", promoname: " + promoname + ", version: " + version + ", stationcode: " + stationcode );

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    con.Open();
                    if (chargetype == 0)
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpformsglobal.charges c inner join kpformsglobal.headercharges hc on hc.currID = c.Type where hc.cEffective = 1;";
                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                    else if (chargetype == 1)
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpformsglobal.ratesperbranchcharges c inner join kpformsglobal.ratesperbranchheader hc on hc.currID = c.Type where hc.cEffective = 1 and hc.branchcode = @bcode and hc.zonecode = @zcode;";
                            command.Parameters.AddWithValue("bcode", bcode);
                            command.Parameters.AddWithValue("zcode", zcode);
                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                    else
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpformsglobal.promoratescharges c inner join kpformsglobal.promoratesheader hc on hc.currID = c.Type where hc.promoname = @promoname;";
                            command.Parameters.AddWithValue("promoname", promoname);

                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                }
                catch (MySqlException mex)
                {
                    kplog.Fatal("FAILED:: respcode:  0 , message : " + mex.ToString());
                    throw new Exception(mex.ToString());
                    //return 0;
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            throw new Exception(ex.ToString());
            //return 0;
        }
    }

    [WebMethod]
    public Decimal getMaxAmountDomestic(String Username, String Password, Int16 chargetype, String bcode, Int16 zcode, String promoname, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", chargetype: " + chargetype + ", bcode: " + bcode + ", zcode: " + zcode + ", promoname: " + promoname + ", version: " + version + ", stationcode: " + stationcode);

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}
            using (MySqlConnection con = dbconDomestic.getConnection())
            {
                try
                {
                    con.Open();
                    if (chargetype == 0)
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpforms.charges c inner join kpforms.headercharges hc on hc.currID = c.Type where hc.cEffective = 1;";
                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                    else if (chargetype == 1)
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpforms.ratesperbranchcharges c inner join kpforms.ratesperbranchheader hc on hc.currID = c.Type where hc.cEffective = 1 and hc.branchcode = @bcode and hc.zonecode = @zcode;";
                            command.Parameters.AddWithValue("bcode", bcode);
                            command.Parameters.AddWithValue("zcode", zcode);
                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                    else
                    {
                        using (command = con.CreateCommand())
                        {
                            command.CommandText = "select MAX(c.MaxAmount) as maximum from kpforms.promoratescharges c inner join kpforms.promoratesheader hc on hc.currID = c.Type where hc.promoname = @promoname;";
                            command.Parameters.AddWithValue("promoname", promoname);

                            using (MySqlDataReader dataReader = command.ExecuteReader())
                            {
                                dataReader.Read();
                                Decimal maximum = Convert.ToDecimal(dataReader["maximum"]);
                                dataReader.Close();
                                con.Close();

                                return maximum;
                            }
                        }
                    }
                }
                catch (MySqlException mex)
                {
                    kplog.Fatal("FAILED:: respcode : 0 , message : " + mex.ToString());
                    throw new Exception(mex.ToString());
                    //return 0;
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode : 0 , message : " + ex.ToString());
            throw new Exception(ex.ToString());
            //return 0;
        }
    }

    [WebMethod]
    public MessagesResponse globalMessages(String Username, String Password, String type)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", type: " + type);

        try
        {
            using (MySqlConnection con = dbconGlobal.getConnection())
            {
                try
                {
                    String[] messages = new String[2];
                    con.Open();

                    using (command = con.CreateCommand())
                    {
                        command.CommandText = "Select message from kpformsglobal.txtmessages where type = @type order by purpose";
                        command.Parameters.AddWithValue("type", type);
                        using (MySqlDataReader dataReader = command.ExecuteReader())
                        {
                            if (dataReader.HasRows)
                            {
                                int x = 0;
                                while (dataReader.Read())
                                {
                                    messages[x] = dataReader["message"].ToString();
                                    x = x + 1;
                                }
                                dataReader.Close();
                                con.Close();
                                kplog.Info("SUCCESS:: respcode: 1 , message : " + getRespMessage(1) + " , txtmessage : " + messages);
                                return new MessagesResponse { respcode = 1, message = getRespMessage(1), txtmessage = messages };
                            }
                            else
                            {
                                dataReader.Close();
                                con.Close();
                                String warningmessage = "Type not found";
                                kplog.Error("FAILED:: respcode : 0 , message : " + warningmessage);
                                return new MessagesResponse { respcode = 0, message = warningmessage };
                            }

                        }
                    }
                }
                catch (MySqlException mex)
                {
                    kplog.Fatal("FAILED:: respcode:  0 , message : " + mex.ToString());
                    con.Close();
                    return new MessagesResponse { respcode = 0, message = mex.Message, ErrorDetail = mex.ToString() };
                    //return 0;
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode : 0 , message : " + ex.ToString());
            return new MessagesResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
            //return 0;
        }
    }

    [WebMethod]
    public String serverDateGlobal(String Username, String Password, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", version: " + version + ", stationcode: " + stationcode);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                String warningmessage = "Invalid credentials";
                throw new Exception(warningmessage);
            }

            DateTime date = getServerDateGlobal(false);

            return date.ToString("yyyy-MM-dd HH:mm:ss");
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    [WebMethod]
    public String serverDateDomestic(String Username, String Password, Double version, String stationcode)
    {

        kplog.Info("Username: " + Username + ", Password: " + Password + ", version: " + version + ", stationcode: " + stationcode);
        try
        {

            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                String warningmessage = "Invalid credentials";
                throw new Exception(warningmessage);
            }

            DateTime date = getServerDateDomestic(false);

            return date.ToString("yyyy-MM-dd HH:mm:ss");
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode : 0 , message: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    [WebMethod]
    public kptnResponse getKptnGlobalMobile(String Username, String Password, String BranchCode, Int32 ZoneCode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", BranchCode: " + BranchCode + ", ZoneCode: " + ZoneCode + ",version : " + version + ", stationcode: " + stationcode);
        
        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new kptnResponse { respcode = 10, message = getRespMessage(10) };
            //}
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new kptnResponse { respcode = 7, message = getRespMessage(7) };
            }
            kplog.Info("SUCCESS:: respcode : 1 , message : " + getRespMessage(1) + " , KPTN: " + generateKPTNGlobalMobile(BranchCode, ZoneCode));
            return new kptnResponse { respcode = 1, message = getRespMessage(1), kptn = generateKPTNGlobalMobile(BranchCode, ZoneCode) };
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            return new kptnResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public kptnResponse getKptnGlobal(String Username, String Password, String BranchCode, Int32 ZoneCode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", BranchCode: " + BranchCode + ", ZoneCode: " + ZoneCode + ",version : " + version + ", stationcode: " + stationcode);

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new kptnResponse { respcode = 10, message = getRespMessage(10) };
            //}
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new kptnResponse { respcode = 7, message = getRespMessage(7) };
            }
            kplog.Info("SUCCESS:: respcode : 1 , message : " + getRespMessage(1) + " , KPTN: " + generateKPTNGlobal(BranchCode, ZoneCode));
            return new kptnResponse { respcode = 1, message = getRespMessage(1), kptn = generateKPTNGlobal(BranchCode, ZoneCode) };
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message: " + ex.ToString());
            return new kptnResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public kptnResponse getKptnDomestic(String Username, String Password, String BranchCode, Int32 ZoneCode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", BranchCode: " + BranchCode + ", ZoneCode: " + ZoneCode + ",version : " + version + ", stationcode: " + stationcode);

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new kptnResponse { respcode = 10, message = getRespMessage(10) };
            //}
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new kptnResponse { respcode = 7, message = getRespMessage(7) };
            }
            kplog.Info("SUCCESS:: respcode : 1 , message : " + getRespMessage(1) + " , KPTN: " + generateKPTNDomestic(BranchCode, ZoneCode));
            return new kptnResponse { respcode = 1, message = getRespMessage(1), kptn = generateKPTNDomestic(BranchCode, ZoneCode) };
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message: " + ex.ToString());
            return new kptnResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public CustomerSearchResponse getPhonenoGlobal(String Username, String Password, String CustID, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", CustID: " + CustID + ", version: " + version + ",stationcode : " + stationcode);
        try
        {

            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                throw new Exception("Invalid credentials");
            }
            using (MySqlConnection custconn = custconGlobal.getConnection())
            {
                try
                {
                    custconn.Open();
                    using (custcommand = custconn.CreateCommand())
                    {
                        //int counter = 0;
                        String query = "Select c.PhoneNo,c.Email,DATE_FORMAT(c.IssuedDate,'%Y-%m-%d') as IssuedDate,c.PlaceIssued,c.ZipCode,cc.SecondIDType,cc.SecondIDNo,cc.SecondPlaceIssued,cc.SecondIssuedDate,cc.SecondExpiryDate,cc.HomeCity,cc.WorkStreet,cc.WorkProvinceCity,cc.WorkCity,cc.WorkCountry,cc.WorkZipCode,cc.Occupation,cc.SSN,cc.SourceOfIncome,cc.Relation,cc.ProofOfFunds from kpcustomersglobal.customers c left join kpcustomersglobal.customersdetails cc on c.CustID = cc.CustID where c.CustID = @CustID";
                        custcommand.CommandText = query;
                        custcommand.Parameters.AddWithValue("CustID", CustID);
                        using (MySqlDataReader ReaderCount = custcommand.ExecuteReader())
                        {
                            String phoneno;
                            String email;
                            String dtissued;
                            String placeissued;
                            String zipcode;
                            String secidtype;
                            String secidno;
                            String secplaceissued;
                            String secissueddate;
                            String secexpirydate;
                            String homecity;
                            String wrkstreet;
                            String wrkprovcity;
                            String wrkcity;
                            String wrkcountry;
                            String wrkzipcode;
                            String occupation;
                            String ssn;
                            String sourceincome;
                            String relation;
                            String proof;
                            if (ReaderCount.Read())
                            {
                                phoneno = ReaderCount["PhoneNo"].ToString();
                                email = ReaderCount["Email"].ToString();
                                dtissued = ReaderCount["IssuedDate"].ToString();
                                placeissued = ReaderCount["PlaceIssued"].ToString();
                                zipcode = ReaderCount["ZipCode"].ToString();
                                secidtype = ReaderCount["SecondIDType"].ToString();
                                secidno = ReaderCount["SecondIDNo"].ToString();
                                secplaceissued = ReaderCount["SecondPlaceIssued"].ToString();
                                secissueddate = (ReaderCount["SecondIssuedDate"].Equals(DBNull.Value) || ReaderCount["SecondIssuedDate"].Equals("") || ReaderCount["SecondIssuedDate"].ToString().StartsWith("0/")) ? String.Empty : Convert.ToDateTime(ReaderCount["SecondIssuedDate"]).ToString("yyyy-MM-dd");
                                secexpirydate = (ReaderCount["SecondExpiryDate"].Equals(DBNull.Value) || ReaderCount["SecondExpiryDate"].Equals("") || ReaderCount["SecondExpiryDate"].ToString().StartsWith("0/")) ? String.Empty : Convert.ToDateTime(ReaderCount["SecondExpiryDate"]).ToString("yyyy-MM-dd");
                                homecity = ReaderCount["HomeCity"].ToString();
                                wrkstreet = ReaderCount["WorkStreet"].ToString();
                                wrkprovcity = ReaderCount["WorkProvinceCity"].ToString();
                                wrkcity = ReaderCount["WorkCity"].ToString();
                                wrkcountry = ReaderCount["WorkCountry"].ToString();
                                wrkzipcode = ReaderCount["WorkZipCode"].ToString();
                                occupation = ReaderCount["Occupation"].ToString();
                                ssn = ReaderCount["SSN"].ToString();
                                sourceincome = ReaderCount["SourceOfIncome"].ToString();
                                relation = ReaderCount["Relation"].ToString();
                                proof = ReaderCount["ProofOfFunds"].ToString();
                                custconn.Close();
                                ReaderCount.Close();
                                kplog.Info("SUCCESS:: message : " + " PhoneNo: " + phoneno + ", Email : " + email + ", dtissued : " + dtissued + ", placeissued : " + placeissued + ", Zipcode: " + zipcode + ", secondidtype: " + secidtype + ", secondidno : " + secidno + ", secondplaceissued : " + secplaceissued + ", secondissueddate :" + secissueddate + ", secondexpirydate : " + secexpirydate + ", homecity: " + homecity + ", workstreet: " + wrkstreet + ", workprovcity: " + wrkprovcity + ", workcity : " + wrkcity + ", workcountry: " + wrkcountry + ", workzipcode: " + wrkzipcode + ", occupation: " + occupation + ", ssn :" + ssn + ", sourceofincome : " + sourceincome + ", relation: " + relation + ", proofoffunds: " + proof);
                                return new CustomerSearchResponse { PhoneNo = phoneno, Email = email, dtissued = dtissued, placeissued = placeissued, Zipcode = zipcode, secondidtype = secidtype, secondidno = secidno, secondplaceissued = secplaceissued, secondissueddate = secissueddate, secondexpirydate = secexpirydate, homecity = homecity, workstreet = wrkstreet, workprovcity = wrkprovcity, workcity = wrkcity, workcountry = wrkcountry, workzipcode = wrkzipcode, occupation = occupation, ssn = ssn, sourceofincome = sourceincome, relation = relation, proofoffunds = proof };
                            }
                            else
                            {
                                custconn.Close();
                                ReaderCount.Close();
                                kplog.Error("FAILED:: respcode: 0 , message : No Data Found!");
                                return new CustomerSearchResponse { respcode = 0, message = "No Data Found" };
                            }
                        }
                    }

                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode : 0 , message : " + ex.ToString());
                    custconn.Close();
                    throw new Exception(ex.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            return new CustomerSearchResponse { respcode = 0, message = ex.ToString() };
            throw new Exception(ex.ToString());

        }

    }

    [WebMethod]
    public String getPhoneno(String Username, String Password, String CustID, Double version, String stationcode)
    {

        kplog.Info("Username: " + Username + ", Password: " + Password + ", CustID: " + CustID + ", version: " + version + ",stationcode : " + stationcode);

        try
        {

            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                throw new Exception("Invalid credentials");
            }
            using (MySqlConnection custconn = custconDomestic.getConnection())
            {
                try
                {
                    custconn.Open();
                    using (custcommand = custconn.CreateCommand())
                    {
                        //int counter = 0;
                        String query = "Select PhoneNo from kpcustomers.customers where CustID = @CustID";
                        custcommand.CommandText = query;
                        custcommand.Parameters.AddWithValue("CustID", CustID);
                        using (MySqlDataReader ReaderCount = custcommand.ExecuteReader())
                        {
                            String phoneno;
                            if (ReaderCount.Read())
                            {
                                phoneno = ReaderCount["PhoneNo"].ToString();
                                custconn.Close();
                                ReaderCount.Close();
                                return phoneno;
                            }
                            else
                            {
                                custconn.Close();
                                ReaderCount.Close();
                                return String.Empty;
                            }
                        }
                    }

                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode : 0 , message: " + ex.ToString());
                    custconn.Close();
                    throw new Exception(ex.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode : 0 , message : " + ex.ToString());
            return String.Empty;
            throw new Exception(ex.ToString());

        }

    }


    //[WebMethod]
    //public CustomerUpdateResponse updateCustomerGlobal(String Username, String Password, String CustID, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderBirthdate, String SenderBranchID, String IDType, String IDNo, String ExpiryDate, String ModifiedBy, String PhoneNo, String MobileNo, String Email, String CardNo, Double version, String stationcode)
    //{
    //    if (!authenticate(Username, Password))
    //    {
    //        return new CustomerUpdateResponse { respcode = 7, message = getRespMessage(7) };
    //    }
    //    //if (!compareVersions(getVersion(stationcode), version))
    //    //{
    //    //    return new CustomerUpdateResponse { respcode = 10, message = getRespMessage(10) };
    //    //}
    //    try
    //    {
    //        dt = getServerDateGlobal(false);
    //    }
    //    catch (Exception ex)
    //    {
    //        kplog.Info("Exeption catch");
    //        return new CustomerUpdateResponse { respcode = 0, message = ex.ToString() };
    //    }
    //    using (MySqlConnection custconn = custconGlobal.getConnection())
    //    {
    //        try
    //        {
    //            custconn.Open();

    //            using (custcommand = custconn.CreateCommand())
    //            {

    //                String query = "UPDATE kpcustomersglobal.customers a SET a.FirstName = @FirstName , a.LastName = @LastName , a.MiddleName = @MiddleName , a.Street = @Street , a.ProvinceCity = @ProvinceCity , a.Country = @Country , a.Gender = @Gender , a.Birthdate = @Birthdate , a.IDType = @IDType , a.IDNo = @IDNo , a.ExpiryDate = @ExpiryDate , a.DTModified = @DTModified, a.ModifiedBy = @ModifiedBy, a.PhoneNo = @PhoneNo, a.Mobile = @Mobile, a.Email = @Email WHERE a.CustID = @CustID;";
    //                custcommand.CommandText = query;
    //                //custcommand.Parameters.AddWithValue("CardNo", CardNo);
    //                custcommand.Parameters.AddWithValue("FirstName", SenderFName);
    //                custcommand.Parameters.AddWithValue("LastName", SenderLName);
    //                custcommand.Parameters.AddWithValue("MiddleName", SenderMName);
    //                custcommand.Parameters.AddWithValue("Street", SenderStreet);
    //                custcommand.Parameters.AddWithValue("ProvinceCity", SenderProvinceCity);
    //                custcommand.Parameters.AddWithValue("Country", SenderCountry);
    //                custcommand.Parameters.AddWithValue("Gender", SenderGender);
    //                //custcommand.Parameters.AddWithValue("ContactNo", SenderContactNo);
    //                custcommand.Parameters.AddWithValue("Birthdate", SenderBirthdate);
    //                //custcommand.Parameters.AddWithValue("BranchID", SenderBranchID);
    //                custcommand.Parameters.AddWithValue("IDType", IDType);
    //                custcommand.Parameters.AddWithValue("IDNo", IDNo);
    //                custcommand.Parameters.AddWithValue("ExpiryDate", (ExpiryDate.Equals(String.Empty)) ? null : ExpiryDate);
    //                custcommand.Parameters.AddWithValue("DTModified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
    //                custcommand.Parameters.AddWithValue("CustID", CustID);
    //                custcommand.Parameters.AddWithValue("ModifiedBy", ModifiedBy);
    //                custcommand.Parameters.AddWithValue("PhoneNo", PhoneNo);
    //                custcommand.Parameters.AddWithValue("Mobile", MobileNo);
    //                custcommand.Parameters.AddWithValue("Email", Email);
    //                custcommand.ExecuteNonQuery();

    //                using (custcommand = custconn.CreateCommand())
    //                {
    //                    if (CustID != String.Empty)
    //                    {
    //                        custcommand.CommandText = "Select CardNo from kpcustomersglobal.customercard where CardNo = @cardnumber";
    //                        custcommand.Parameters.AddWithValue("cardnumber", CardNo);
    //                        using (MySqlDataReader dataReader = custcommand.ExecuteReader())
    //                        {
    //                            if (!dataReader.Read())
    //                            {
    //                                dataReader.Close();
    //                                if (!CardNo.Equals(String.Empty))
    //                                {
    //                                    String queryCard = "INSERT INTO kpcustomersglobal.customercard (`CardNo`) values (@CardNo1);";
    //                                    custcommand.CommandText = queryCard;
    //                                    custcommand.Parameters.AddWithValue("CardNo1", CardNo);
    //                                    custcommand.ExecuteNonQuery();

    //                                    custcommand.Parameters.Clear();
    //                                    String queryCard1 = "UPDATE kpcustomersglobal.customers SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
    //                                    custcommand.CommandText = queryCard1;
    //                                    custcommand.Parameters.AddWithValue("CardNo1", CardNo);
    //                                    custcommand.Parameters.AddWithValue("CustID1", CustID);
    //                                    custcommand.ExecuteNonQuery();
    //                                }
    //                            }
    //                            else
    //                            {
    //                                dataReader.Close();
    //                                //String queryCard = "UPDATE kpcustomersglobal.customercard SET CardNo = @CardNo1 WHERE CardNo = @CustID1;";
    //                                String queryCard = "UPDATE kpcustomersglobal.customercard cc INNER JOIN kpcustomersglobal.customers c ON cc.cardno=c.cardno SET cc.CardNo = @CardNo1 WHERE c.CardNo = @CustID1;";
    //                                custcommand.CommandText = queryCard;
    //                                custcommand.Parameters.AddWithValue("CardNo1", CardNo);
    //                                custcommand.Parameters.AddWithValue("CustID1", CustID);

    //                                custcommand.ExecuteNonQuery();
    //                                custcommand.Parameters.Clear();
    //                                String queryCard1 = "UPDATE kpcustomersglobal.customers SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
    //                                custcommand.CommandText = queryCard1;
    //                                custcommand.Parameters.AddWithValue("CardNo1", CardNo);
    //                                custcommand.Parameters.AddWithValue("CustID1", CustID);
    //                                custcommand.ExecuteNonQuery();
    //                            }
    //                        }
    //                    }

    //                    custconn.Close();

    //                    return new CustomerUpdateResponse { respcode = 1, message = getRespMessage(1) };
    //                }
    //            }
    //        }
    //        catch (Exception ex)
    //        {
    //            kplog.Fatal("Duplicate", ex);
    //            custconn.Close();
    //            int respcode = 0;
    //            if (ex.Message.StartsWith("Duplicate"))
    //            {
    //                respcode = 6;
    //            }
    //            return new CustomerUpdateResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = ex.ToString() };
    //        }
    //    }
    //}

    [WebMethod]
    public CustomerUpdateResponse updateCustomerGlobal(String Username, String Password, String CustID, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String ZipCode, String SenderGender, String SenderBirthdate, String SenderBranchID, String IDType, String IDNo, String ExpiryDate, String ModifiedBy, String PhoneNo, String MobileNo, String Email, String CardNo, Double version, String stationcode, String dtissue, String placeissue, String sidtype, String sidno, String splaceissue, String sdtissue, String sexpirydate, String sworkstreet, String sworkprovcity, String sworkcountry, String sworkzipcode, String soccupation, String sssn, String ssourceofincome, String srelation, String sproofoffunds, String shomecity, String sworkcity)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", CustID: " + CustID + ", SenderFName: " + SenderFName + ",SenderLName : " + SenderLName + ", SenderMName: " + SenderMName + ",SenderStreet : " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ",ZipCode : " + ZipCode + ", SenderGender: " + SenderGender + " ,SenderBirthdate :"+SenderBirthdate+", SenderBranchID: "+SenderBranchID+", IDType: "+IDType+", IDNo: "+IDNo+", ExpiryDate: "+ExpiryDate+", Modifiedby: "+ModifiedBy+", PhoneNo: "+PhoneNo+", MobileNo: "+MobileNo+", Email: "+Email+", CardNo: "+CardNo+", version: "+version+", stationcode: "+stationcode+", dttissue: "+dtissue+", placeissue: "+placeissue+", sidtype: "+sidtype+", sidno: "+sidno+", splaceissue: "+splaceissue+", sdtissue: "+sdtissue+", sexpirydate: "+sexpirydate+", sworkstreet: "+sworkstreet+", sworkprovcity: "+sworkprovcity+", sworkcountry: "+sworkcountry+", sworkzipcode: "+sworkzipcode+", soccupation: "+soccupation+", sssn: "+sssn+", ssourceofincome: "+ssourceofincome+", srelation: "+srelation+", sproofoffunds: "+sproofoffunds+", shomecity: "+shomecity+", sworkcity: "+sworkcity);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new CustomerUpdateResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new CustomerUpdateResponse { respcode = 10, message = getRespMessage(10) };
        //}
        //if (verifyCustomer(SenderFName, SenderLName, SenderMName, SenderBirthdate))
        //{
        //    return new CustomerUpdateResponse { respcode = 6, message = getRespMessage(6) };
        //}
        try
        {
            dt = getServerDateGlobal(false);
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode : 0 , message: " + ex.ToString());
            return new CustomerUpdateResponse { respcode = 0, message = ex.ToString() };
        }
        using (MySqlConnection custconn = custconGlobal.getConnection())
        {
            try
            {
                custconn.Open();

                using (custcommand = custconn.CreateCommand())
                {
                    //String qrychkblockedtrxn = "Select SenderLName, SenderFName, SenderMName from kpglobal.blockedtransactions where SenderFName=@bfname and SenderLName=@blname and SenderMName=@bmname";
                    //custcommand.CommandText = qrychkblockedtrxn;
                    //custcommand.Parameters.Clear();
                    //custcommand.Parameters.AddWithValue("bfname",SenderFName);
                    //custcommand.Parameters.AddWithValue("blname",SenderLName);
                    //custcommand.Parameters.AddWithValue("bmname", SenderMName);
                    //MySqlDataReader rdrchkblock = custcommand.ExecuteReader();
                    //if (!(rdrchkblock.Read()))
                    //{
                    //rdrchkblock.Close();

                    String chkcustomers = "SELECT FirstName FROM kpcustomersglobal.customers where FirstName = @pfname and LastName = @plname and MiddleName = @pmname and DATE_FORMAT(Birthdate,'%Y-%m-%d') = @sbdate and CustID != @pcustid";
                    custcommand.CommandText = chkcustomers;
                    custcommand.Parameters.Clear();
                    custcommand.Parameters.AddWithValue("pfname", SenderFName);
                    custcommand.Parameters.AddWithValue("plname", SenderLName);
                    custcommand.Parameters.AddWithValue("pmname", SenderMName);
                    custcommand.Parameters.AddWithValue("sbdate", SenderBirthdate);
                    custcommand.Parameters.AddWithValue("pcustid", CustID);
                    MySqlDataReader rdrchkcust = custcommand.ExecuteReader();
                    if (rdrchkcust.Read())
                    {
                        return new CustomerUpdateResponse { respcode = 6, message = getRespMessage(6) };
                    }
                    rdrchkcust.Close();

                    String query = "UPDATE kpcustomersglobal.customers a SET a.FirstName = @FirstName , a.LastName = @LastName , a.MiddleName = @MiddleName , a.Street = @Street , a.ProvinceCity = @ProvinceCity , a.Country = @Country , a.ZipCode = @Zipcode , a.Gender = @Gender , a.Birthdate = @Birthdate , a.IDType = @IDType , a.IDNo = @IDNo , a.ExpiryDate = @ExpiryDate , a.DTModified = @DTModified, a.ModifiedBy = @ModifiedBy, a.PhoneNo = @PhoneNo, a.Mobile = @Mobile, a.Email = @Email, a.IssuedDate = @dtissued, a.PlaceIssued = @placeissued WHERE a.CustID = @CustID;";
                    custcommand.CommandText = query;
                    //custcommand.Parameters.AddWithValue("CardNo", CardNo);
                    custcommand.Parameters.AddWithValue("FirstName", SenderFName);
                    custcommand.Parameters.AddWithValue("LastName", SenderLName);
                    custcommand.Parameters.AddWithValue("MiddleName", SenderMName);
                    custcommand.Parameters.AddWithValue("Street", SenderStreet);
                    custcommand.Parameters.AddWithValue("ProvinceCity", SenderProvinceCity);
                    custcommand.Parameters.AddWithValue("Country", SenderCountry);
                    custcommand.Parameters.AddWithValue("Zipcode", ZipCode);
                    custcommand.Parameters.AddWithValue("Gender", SenderGender);
                    //custcommand.Parameters.AddWithValue("ContactNo", SenderContactNo);
                    custcommand.Parameters.AddWithValue("Birthdate", SenderBirthdate);
                    //custcommand.Parameters.AddWithValue("BranchID", SenderBranchID);
                    custcommand.Parameters.AddWithValue("IDType", IDType);
                    custcommand.Parameters.AddWithValue("IDNo", IDNo);
                    custcommand.Parameters.AddWithValue("ExpiryDate", (ExpiryDate.Equals(String.Empty)) ? null : ExpiryDate);
                    custcommand.Parameters.AddWithValue("DTModified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    custcommand.Parameters.AddWithValue("CustID", CustID);
                    custcommand.Parameters.AddWithValue("ModifiedBy", ModifiedBy);
                    custcommand.Parameters.AddWithValue("PhoneNo", PhoneNo);
                    custcommand.Parameters.AddWithValue("Mobile", MobileNo);
                    custcommand.Parameters.AddWithValue("Email", Email);
                    custcommand.Parameters.AddWithValue("dtissued", (dtissue.Equals(String.Empty)) ? null : dtissue);
                    custcommand.Parameters.AddWithValue("placeissued", placeissue);
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: respcode: 1 , message : UPDATE kpcustomersglobal.customers: "
                    + " FirstName: " + SenderFName
                    + " LastName: " + SenderLName
                    + " MiddleName: " + SenderMName
                    + " Street: " + SenderStreet
                    + " ProvinceCity: " + SenderProvinceCity
                    + " Country: " + SenderCountry
                    + " Zipcode: " + ZipCode
                    + " Gender: " + SenderGender
                    + " Birthdate: " + SenderBirthdate
                    + " IDType: " + IDType
                    + " IDNo: " + IDNo
                    + " ExpiryDate: " + (ExpiryDate == String.Empty ? null : ExpiryDate)
                    + " DTModified: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                    + " CustID: " + CustID
                    + " ModifiedBy: " + ModifiedBy
                    + " PhoneNo: " + PhoneNo
                    + " Mobile: " + MobileNo
                    + " Email: " + Email
                    + " dtissued: " + (dtissue == String.Empty ? null : dtissue)
                    + " placeissued: " + placeissue);

                    String qryaddupcustomerdetails = "Select CustID from kpcustomersglobal.customersdetails where CustID = @idcust";
                    custcommand.CommandText = qryaddupcustomerdetails;
                    custcommand.Parameters.Clear();
                    custcommand.Parameters.AddWithValue("idcust", CustID);
                    MySqlDataReader rdrcust = custcommand.ExecuteReader();
                    if (rdrcust.Read())
                    {
                        rdrcust.Close();
                        String qryupdatecustomerdetails = "update kpcustomersglobal.customersdetails set SecondIDType = @usidtype, SecondIDNo = @usidno, SecondPlaceIssued = @usplaceissued, SecondIssuedDate = @usdtissued, SecondExpiryDate = @usexpirydate, HomeCity = @uhomecity, WorkStreet = @uworkstreet, WorkProvinceCity = @uworkprovcity, WorkCity = @uworkcity, WorkCountry = @uworkcountry, WorkZipCode = @uworkzipcode, Occupation = @uoccupation, SSN = @ussn, SourceOfIncome = @usourceofincome, Relation = @urelation, ProofOfFunds = @uproofoffunds where CustID = @ucustid";
                        custcommand.CommandText = qryupdatecustomerdetails;
                        custcommand.Parameters.AddWithValue("ucustid", CustID);
                        custcommand.Parameters.AddWithValue("usidtype", sidtype);
                        custcommand.Parameters.AddWithValue("usidno", sidno);
                        custcommand.Parameters.AddWithValue("usplaceissued", splaceissue);
                        custcommand.Parameters.AddWithValue("usdtissued", (sdtissue.Equals(String.Empty)) ? null : sdtissue);
                        custcommand.Parameters.AddWithValue("usexpirydate", (sexpirydate.Equals(String.Empty)) ? null : sexpirydate);
                        custcommand.Parameters.AddWithValue("uhomecity", shomecity);
                        custcommand.Parameters.AddWithValue("uworkstreet", sworkstreet);
                        custcommand.Parameters.AddWithValue("uworkprovcity", sworkprovcity);
                        custcommand.Parameters.AddWithValue("uworkcity", sworkcity);
                        custcommand.Parameters.AddWithValue("uworkcountry", sworkcountry);
                        custcommand.Parameters.AddWithValue("uworkzipcode", sworkzipcode);
                        custcommand.Parameters.AddWithValue("uoccupation", soccupation);
                        custcommand.Parameters.AddWithValue("ussn", sssn);
                        custcommand.Parameters.AddWithValue("usourceofincome", ssourceofincome);
                        custcommand.Parameters.AddWithValue("urelation", srelation);
                        custcommand.Parameters.AddWithValue("uproofoffunds", sproofoffunds);
                        custcommand.ExecuteNonQuery();
                        kplog.Info("SUCCES:: message: Update kpcustomersglobal.customersdetails:: "
                         + " ucustid: " + CustID
                         + " usidtype: " + sidtype
                         + " usidno: " + sidno
                         + " usplaceissued: " + splaceissue
                         + " usdtissued: " +  sdtissue
                         + " usexpirydate: " + sexpirydate
                         + " uhomecity: " + shomecity
                         + " uworkstreet: " + sworkstreet
                         + " uworkprovcity: " + sworkprovcity
                         + " uworkcity: " + sworkcity
                         + " uworkcountry: " + sworkcountry
                         + " uworkzipcode: " + sworkzipcode
                         + " uoccupation: " + soccupation
                         + " ussn: " + sssn
                         + " usourceofincome: " + ssourceofincome
                         + " urelation: " + srelation
                         + " uproofoffunds: " + sproofoffunds);
                    }
                    else
                    {
                        rdrcust.Close();
                        String insertCustomerDetails = "INSERT INTO kpcustomersglobal.customersdetails(CustID,SecondIDType,SecondIDNo,SecondPlaceIssued,SecondIssuedDate,SecondExpiryDate,HomeCity,WorkStreet,WorkProvinceCity,WorkCity,WorkCountry,WorkZipCode,Occupation,SSN,SourceOfIncome,Relation,ProofOfFunds) values(@dcustid,@dsidtype,@dsidno,@dsplaceissued,@dsdateissued,@dsexpirydate,@dhomecity,@dworkstreet,@dworkprovincecity,@dworkcity,@dworkcountry,@dworkzipcode,@doccupation,@dssn,@dsourceofincome,@drelation,@dproofoffunds)";
                        custcommand.CommandText = insertCustomerDetails;
                        custcommand.Parameters.Clear();
                        custcommand.Parameters.AddWithValue("dcustid", CustID);
                        custcommand.Parameters.AddWithValue("dsidtype", sidtype);
                        custcommand.Parameters.AddWithValue("dsidno", sidno);
                        custcommand.Parameters.AddWithValue("dsplaceissued", splaceissue);
                        custcommand.Parameters.AddWithValue("dsdateissued", (sdtissue.Equals(String.Empty)) ? null : sdtissue);
                        custcommand.Parameters.AddWithValue("dsexpirydate", (sexpirydate.Equals(String.Empty)) ? null : sexpirydate);
                        custcommand.Parameters.AddWithValue("dhomecity", shomecity);
                        custcommand.Parameters.AddWithValue("dworkstreet", sworkstreet);
                        custcommand.Parameters.AddWithValue("dworkprovincecity", sworkprovcity);
                        custcommand.Parameters.AddWithValue("dworkcity", sworkcity);
                        custcommand.Parameters.AddWithValue("dworkcountry", sworkcountry);
                        custcommand.Parameters.AddWithValue("dworkzipcode", sworkzipcode);
                        custcommand.Parameters.AddWithValue("doccupation", soccupation);
                        custcommand.Parameters.AddWithValue("dssn", sssn);
                        custcommand.Parameters.AddWithValue("dsourceofincome", ssourceofincome);
                        custcommand.Parameters.AddWithValue("drelation", srelation);
                        custcommand.Parameters.AddWithValue("dproofoffunds", sproofoffunds);
                        custcommand.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: respcode: 1 , message : INSERT INTO kpcustomersglobal.customersdetails: "
                        + " ucustid: " + CustID
                        + " SecondIDType : " + sidtype
                        + " SecondIDNo : " + sidno
                        + " SecondPlaceIssued : " + splaceissue
                        + " SecondIssuedDate : " + (sdtissue == String.Empty ? null : sdtissue)
                        + " SecondExpiryDate : " + (sexpirydate == String.Empty ? null : sexpirydate)
                        + " HomeCity : " + shomecity
                        + " WorkStreet : " + sworkstreet
                        + " WorkProvinceCity : " + sworkprovcity
                        + " WorkCity : " + sworkcity
                        + " WorkCountry: " + sworkcountry
                        + " WorkZipCode : " + sworkzipcode
                        + " Occupation: " + soccupation
                        + " SSN : " + sssn
                        + " SourceOfIncome : " + ssourceofincome
                        + " Relation: " + srelation
                        + " ProofOfFunds: " + sproofoffunds);
                    }
                    rdrcust.Close();
                    //}
                    //else
                    //{
                    //    rdrchkblock.Close();
                    //    custconn.Close();
                    //    return new CustomerUpdateResponse { respcode = 0, message = "Customer cannot be update because it has block transaction record" };
                    //}
                    //rdrchkblock.Close();

                    using (custcommand = custconn.CreateCommand())
                    {
                        if (CustID != String.Empty)
                        {
                            custcommand.CommandText = "Select CardNo from kpcustomersglobal.customercard where CardNo = @cardnumber";
                            custcommand.Parameters.AddWithValue("cardnumber", CardNo);
                            using (MySqlDataReader dataReader = custcommand.ExecuteReader())
                            {
                                if (!dataReader.Read())
                                {
                                    dataReader.Close();
                                    if (!CardNo.Equals(String.Empty))
                                    {
                                        String queryCard = "INSERT INTO kpcustomersglobal.customercard (`CardNo`) values (@CardNo1);";
                                        custcommand.CommandText = queryCard;
                                        custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                        custcommand.ExecuteNonQuery();
                                        kplog.Info("SUCCESS:: respcode: 1 , message : INSERT INTO kpcustomersglobal.customercard:  CardNo: " + CardNo);

                                        custcommand.Parameters.Clear();
                                        String queryCard1 = "UPDATE kpcustomersglobal.customers SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
                                        custcommand.CommandText = queryCard1;
                                        custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                        custcommand.Parameters.AddWithValue("CustID1", CustID);
                                        custcommand.ExecuteNonQuery();
                                        kplog.Info("SUCCESS:: respcode 1 , message : UPDATE kpcustomersglobal.customers: CardNo: " + CardNo + ", CustID" + CustID);
                                    }
                                }
                                else
                                {
                                    dataReader.Close();
                                    //String queryCard = "UPDATE kpcustomersglobal.customercard SET CardNo = @CardNo1 WHERE CardNo = @CustID1;";
                                    String queryCard = "UPDATE kpcustomersglobal.customercard cc INNER JOIN kpcustomersglobal.customers c ON cc.cardno=c.cardno SET cc.CardNo = @CardNo1 WHERE c.CardNo = @CustID1;";
                                    custcommand.CommandText = queryCard;
                                    custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                    custcommand.Parameters.AddWithValue("CustID1", CustID);
                                    custcommand.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: respcode : 1 , message : UPDATE kpcustomersglobal.customercard cc INNER JOIN kpcustomersglobal.customers c ON cc.cardno=c.cardno: cc.CardNo : " + CardNo + ", c.CardNo: " + CustID);
                                    custcommand.Parameters.Clear();

                                    String queryCard1 = "UPDATE kpcustomersglobal.customers SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
                                    custcommand.CommandText = queryCard1;
                                    custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                    custcommand.Parameters.AddWithValue("CustID1", CustID);
                                    custcommand.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: respcode: 1 , message : UPDATE kpcustomersglobal.customers : CardNo : " + CardNo + ", CustID" + CustID);
                                }
                            }
                        }

                        String creator = ModifiedBy.Substring(4);
                        String insertCustomerLogs = "INSERT INTO kpadminlogsglobal.customerlogs(ScustID,Details,Syscreated,Syscreator,Type) values(@scustid,@details,now(),@creator,@type)";
                        custcommand.CommandText = insertCustomerLogs;
                        custcommand.Parameters.Clear();
                        custcommand.Parameters.AddWithValue("scustid", CustID);
                        custcommand.Parameters.AddWithValue("details", "{Name:" + " " + SenderFName + " " + SenderMName + " " + SenderLName + ", " + "Street:" + " " + SenderStreet + ", " + "ProvinceCity:" + " " + SenderProvinceCity + ", " + "ZipCode:" + " " + ZipCode + ", " + "Country:" + " " + SenderCountry + ", " + "Gender:" + " " + SenderGender + ", " + "BirthDate:" + " " + SenderBirthdate + ", " + "IDType:" + " " + IDType + ", " + "IDNo:" + " " + IDNo + ", " + "ExpiryDate:" + " " + ExpiryDate + ", " + "PhoneNo:" + " " + PhoneNo + ", " + "MobileNo:" + " " + MobileNo + ", " + "Email:" + " " + Email + ", " + "MlcardNo:" + " " + CardNo + ", " + "IssuedDate:" + " " + ((dtissue.Equals(String.Empty)) ? null : dtissue) + ", " + "CreatedByBranch" + " " + ModifiedBy + ", " + "PlaceIssued:" + " " + placeissue + "}");
                        custcommand.Parameters.AddWithValue("creator", creator);
                        custcommand.Parameters.AddWithValue("type", "U");
                        custcommand.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: respcode : 1 , message : INSERT INTO kpadminlogsglobal.customerlogs: ScustID: " + CustID
                      + " ,Details: " + "{Name:" + " " + SenderFName + " " + SenderMName + " " + SenderLName + ", " + "Street:" + " " + SenderStreet + ", " + "ProvinceCity:" + " " + SenderProvinceCity + ", " + "ZipCode:" + " " + ZipCode + ", " + "Country:" + " " + SenderCountry + ", " + "Gender:" + " " + SenderGender + ", " + "BirthDate:" + " " + SenderBirthdate + ", " + "IDType:" + " " + IDType + ", " + "IDNo:" + " " + IDNo + ", " + "ExpiryDate:" + " " + ExpiryDate + ", " + "PhoneNo:" + " " + PhoneNo + ", " + "MobileNo:" + " " + MobileNo + ", " + "Email:" + " " + Email + ", " + "MlcardNo:" + " " + CardNo + ", " + "IssuedDate:" + " " + ((dtissue.Equals(String.Empty)) ? null : dtissue) + ", " + "CreatedByBranch" + " " + ModifiedBy + ", " + "PlaceIssued:" + " " + placeissue + "}"
                      + " ,Syscreated: now()"
                      + " ,Syscreator: " + creator
                      + " ,Type: U");

                        custconn.Close();
                        kplog.Info("SUCCESS:: respcode: 1, message : " + getRespMessage(1));
                        return new CustomerUpdateResponse { respcode = 1, message = getRespMessage(1) };
                    }
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 , message : Duplicate , " + ex.ToString());
                custconn.Close();
                int respcode = 0;
                if (ex.Message.StartsWith("Duplicate"))
                {
                    respcode = 6;
                }
                kplog.Fatal("FAILED:: respocde: " + respcode + ", message : " + getRespMessage(respcode) + " , ErrorDetail: " + ex.ToString());
                return new CustomerUpdateResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public CustomerUpdateResponse updateCustomer(String Username, String Password, String CustID, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderBirthdate, String SenderBranchID, String IDType, String IDNo, String ExpiryDate, String ModifiedBy, String PhoneNo, String MobileNo, String Email, String CardNo, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", CustID: " + CustID + ", SenderFName: " + SenderFName + ",SenderLName : " + SenderLName + ", SenderMName: " + SenderMName + ",SenderStreet : " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry+ ", SenderGender: " + SenderGender + " ,SenderBirthdate :" + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", IDType: " + IDType + ", IDNo: " + IDNo  + ", ModifiedBy: "+ModifiedBy+", PhoneNO: "+PhoneNo+", MobileNo: "+MobileNo+", Email: "+Email+", CardNo: "+CardNo+", version: "+version+", stationcode: "+stationcode);
        
        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new CustomerUpdateResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new CustomerUpdateResponse { respcode = 10, message = getRespMessage(10) };
        //}
        try
        {
            dt = getServerDateDomestic(false);
        }
        catch (Exception ex)
        {
            kplog.Info("Exeption catch");
            kplog.Fatal("FAILED:: respocde: 0 , message : " + ex.ToString());
            return new CustomerUpdateResponse { respcode = 0, message = ex.ToString() };
        }
        using (MySqlConnection custconn = custconDomestic.getConnection())
        {
            try
            {
                custconn.Open();

                using (custcommand = custconn.CreateCommand())
                {

                    String query = "UPDATE kpcustomers.customers a SET a.FirstName = @FirstName , a.LastName = @LastName , a.MiddleName = @MiddleName , a.Street = @Street , a.ProvinceCity = @ProvinceCity , a.Country = @Country , a.Gender = @Gender , a.Birthdate = @Birthdate, a.IDType = @IDType , a.IDNo = @IDNo , a.ExpiryDate = @ExpiryDate , a.DTModified = @DTModified, a.ModifiedBy = @ModifiedBy, a.PhoneNo = @PhoneNo, a.Mobile = @Mobile, a.Email = @Email WHERE a.CustID = @CustID;";
                    custcommand.CommandText = query;
                    //custcommand.Parameters.AddWithValue("CardNo", CardNo);
                    custcommand.Parameters.AddWithValue("FirstName", SenderFName);
                    custcommand.Parameters.AddWithValue("LastName", SenderLName);
                    custcommand.Parameters.AddWithValue("MiddleName", SenderMName);
                    custcommand.Parameters.AddWithValue("Street", SenderStreet);
                    custcommand.Parameters.AddWithValue("ProvinceCity", SenderProvinceCity);
                    custcommand.Parameters.AddWithValue("Country", SenderCountry);
                    custcommand.Parameters.AddWithValue("Gender", SenderGender);
                    //custcommand.Parameters.AddWithValue("ContactNo", SenderContactNo);
                    custcommand.Parameters.AddWithValue("Birthdate", SenderBirthdate);
                    custcommand.Parameters.AddWithValue("BranchID", SenderBranchID);
                    custcommand.Parameters.AddWithValue("IDType", IDType);
                    custcommand.Parameters.AddWithValue("IDNo", IDNo);
                    custcommand.Parameters.AddWithValue("ExpiryDate", (ExpiryDate.Equals(String.Empty)) ? null : ExpiryDate);
                    custcommand.Parameters.AddWithValue("DTModified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    custcommand.Parameters.AddWithValue("CustID", CustID);
                    custcommand.Parameters.AddWithValue("ModifiedBy", ModifiedBy);
                    custcommand.Parameters.AddWithValue("PhoneNo", PhoneNo);
                    custcommand.Parameters.AddWithValue("Mobile", MobileNo);
                    custcommand.Parameters.AddWithValue("Email", Email);
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: respcode: 1 , message : UPDATE kpcustomers.customers: "
                    + " FirstName: " + SenderFName
                    + " LastName: " + SenderLName
                    + " MiddleName: " + SenderMName
                    + " Street: " + SenderStreet
                    + " ProvinceCity: " + SenderProvinceCity
                    + " Country: " + SenderCountry
                    + " Gender: " + SenderGender
                    + " Birthdate: " + SenderBirthdate
                    + " BranchID: " + SenderBranchID
                    + " IDType: " + IDType
                    + " IDNo: " + IDNo
                    + " ExpiryDate: " + (ExpiryDate == String.Empty ? null : ExpiryDate)
                    + " DTModified: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                    + " CustID: " + CustID
                    + " ModifiedBy: " + ModifiedBy
                    + " PhoneNo: " + PhoneNo
                    + " Mobile: " + MobileNo
                    + " Email: " + Email);

                    using (custcommand = custconn.CreateCommand())
                    {
                        if (CustID != String.Empty)
                        {
                            custcommand.CommandText = "Select CardNo from kpcustomers.customercard where CardNo = @cardnumber";
                            custcommand.Parameters.AddWithValue("cardnumber", CardNo);
                            using (MySqlDataReader dataReader = custcommand.ExecuteReader())
                            {
                                if (!dataReader.Read())
                                {
                                    dataReader.Close();
                                    if (!CardNo.Equals(String.Empty))
                                    {
                                        String queryCard = "INSERT INTO kpcustomers.customercard (`CardNo`) values (@CardNo1);";
                                        custcommand.CommandText = queryCard;
                                        custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                        custcommand.ExecuteNonQuery();
                                        kplog.Info("SUCCESS:: respcode : 1 , message : INSERT INTO kpcustomers.customercard: CardNo : " + CardNo);

                                        custcommand.Parameters.Clear();
                                        String queryCard1 = "UPDATE kpcustomers.customers SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
                                        custcommand.CommandText = queryCard1;
                                        custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                        custcommand.Parameters.AddWithValue("CustID1", CustID);
                                        custcommand.ExecuteNonQuery();
                                        kplog.Info("SUCCES:: respcode: 1,  message : UPDATE kpcustomers.customers: CardNo : " + CardNo + ", CustID" + CustID);
                                    }
                                }
                                else
                                {
                                    dataReader.Close();
                                    String queryCard = "UPDATE kpcustomers.customercard SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
                                    custcommand.CommandText = queryCard;
                                    custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                    custcommand.Parameters.AddWithValue("CustID1", CustID);
                                    custcommand.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: respcode: 1 , message : UPDATE kpcustomers.customercard: CardNo: " + CardNo + ", CustID: " + CustID);
                                    custcommand.Parameters.Clear();
                                    String queryCard1 = "UPDATE kpcustomers.customers SET CardNo = @CardNo1 WHERE CustID = @CustID1;";
                                    custcommand.CommandText = queryCard1;
                                    custcommand.Parameters.AddWithValue("CardNo1", CardNo);
                                    custcommand.Parameters.AddWithValue("CustID1", CustID);
                                    custcommand.ExecuteNonQuery();
                                    kplog.Info("SUCCES:: respcode: 1 , message : UPDATE kpcustomers.customers: CardNo: " + CardNo + ", CustID" + CustID);
                                }
                            }
                        }



                        custconn.Close();
                        kplog.Info("SUCCES:: respcode: 1 , message : " + getRespMessage(1));
                        return new CustomerUpdateResponse { respcode = 1, message = getRespMessage(1) };
                    }
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("Duplicate", ex);
                custconn.Close();
                int respcode = 0;
                if (ex.Message.StartsWith("Duplicate"))
                {
                    respcode = 6;
                }
                kplog.Fatal("FAILED:: respcode: 0 , message: " + getRespMessage(respcode) + ", ErrorDetail: " + ex.ToString());
                return new CustomerUpdateResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = ex.ToString() };
            }
        }
    }


    [WebMethod(BufferResponse = false)]
    public CustomerResultResponse mlcardSearchGlobal(String Username, String Password, String mlcard, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", mlcard: " + mlcard + ", version: " + version + ",stationcode : " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new CustomerResultResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new CustomerResultResponse { respcode = 10, message = getRespMessage(10) };
        //}
        try
        {
            using (MySqlConnection custconn = custconGlobal.getConnection())
            {
                try
                {
                    custconn.Open();
                    using (custcommand = custconn.CreateCommand())
                    {
                        //int counter = 0;
                        //String query = "SELECT c.FirstName, c.LastName, c.MiddleName, c.Street, c.ProvinceCity, c.Birthdate, c.Country,c.ExpiryDate, c.Gender,c.IDNo,c.IDType,c.CustID, c.PhoneNo, c.Mobile,c.Email ,cc.CardNo,cc.CardNo FROM kpcustomersglobal.customers c INNER JOIN kpcustomersglobal.customercard cc ON c.CustID = cc.CustID WHERE cc.CardNo = @mlcard LIMIT 1;";
                        String query = "SELECT c.FirstName, c.LastName, c.MiddleName, c.Street, c.ProvinceCity, c.Birthdate, c.Country,c.ExpiryDate, c.Gender,c.IDNo,c.IDType,c.CustID, c.PhoneNo, c.Mobile,c.Email ,c.CardNo FROM kpcustomersglobal.customers c WHERE c.CardNo = @mlcard LIMIT 1;";
                        custcommand.CommandText = query;
                        custcommand.Parameters.AddWithValue("mlcard", mlcard);

                        //CustArrayResponse b = new CustArrayResponse();
                        using (MySqlDataReader ReaderCount = custcommand.ExecuteReader())
                        {
                            if (ReaderCount.Read())
                            {
                                CustomerSearchResponse csr = new CustomerSearchResponse { FirstName = ReaderCount["FirstName"].ToString(), LastName = ReaderCount["LastName"].ToString(), MiddleName = ReaderCount["MiddleName"].ToString(), Street = ReaderCount["Street"].ToString(), ProvinceCity = ReaderCount["ProvinceCity"].ToString(), MLCardNo = ReaderCount["CardNo"].ToString(), BirthDate = Convert.ToDateTime(ReaderCount["Birthdate"].ToString()).ToString("yyyy-MM-dd"), ContactNo = ReaderCount["Mobile"].ToString(), Country = ReaderCount["Country"].ToString(), ExpiryDate = (ReaderCount["ExpiryDate"].Equals(DBNull.Value)) ? ReaderCount["ExpiryDate"].ToString() : Convert.ToDateTime(ReaderCount["ExpiryDate"]).ToString("yyyy-MM-dd"), Gender = ReaderCount["Gender"].ToString(), IDNo = ReaderCount["IDNo"].ToString(), IDType = ReaderCount["IDType"].ToString(), CustID = ReaderCount["CustID"].ToString() };
                                ReaderCount.Close();
                                custconn.Close();
                                kplog.Info("SUCCES:: respcode: 1 , message : " + getRespMessage(1) + ", CustomerData: " + csr);
                                return new CustomerResultResponse { respcode = 1, message = getRespMessage(1), CustomerData = csr };
                            }
                            else
                            {
                                kplog.Error(getRespMessage(5));
                                ReaderCount.Close();
                                custconn.Close();
                                kplog.Error("FAILED:: respcode: 5 , message : " + getRespMessage(5));
                                return new CustomerResultResponse { respcode = 5, message = getRespMessage(5) };
                            }
                        }
                    }

                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
                    custconn.Close();
                    return new CustomerResultResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), Data = null };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
            custconGlobal.CloseConnection();
            return new CustomerResultResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), Data = null };
        }

    }

    [WebMethod(BufferResponse = false)]
    public CustomerResultResponse mlcardSearch(String Username, String Password, String mlcard, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", mlcard: " + mlcard + ", version: " + version + ", stationcode : " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Unknown Web Service User!");
            return new CustomerResultResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new CustomerResultResponse { respcode = 10, message = getRespMessage(10) };
        //}
        try
        {
            using (MySqlConnection custconn = custconDomestic.getConnection())
            {
                try
                {
                    custconn.Open();
                    using (custcommand = custconn.CreateCommand())
                    {
                        //int counter = 0;
                        //String query = "SELECT c.FirstName, c.LastName, c.MiddleName, c.Street, c.ProvinceCity, c.Birthdate, c.Country,c.ExpiryDate, c.Gender,c.IDNo,c.IDType,c.CustID, c.PhoneNo, c.Mobile,c.Email ,cc.CardNo,cc.CardNo FROM kpcustomersglobal.customers c INNER JOIN kpcustomersglobal.customercard cc ON c.CustID = cc.CustID WHERE cc.CardNo = @mlcard LIMIT 1;";
                        String query = "SELECT c.FirstName, c.LastName, c.MiddleName, c.Street, c.ProvinceCity, c.Birthdate, c.Country,c.ExpiryDate, c.Gender,c.IDNo,c.IDType,c.CustID, c.PhoneNo, c.Mobile,c.Email ,c.CardNo FROM kpcustomers.customers c WHERE c.CardNo = @mlcard LIMIT 1;";
                        custcommand.CommandText = query;
                        custcommand.Parameters.AddWithValue("mlcard", mlcard);

                        //CustArrayResponse b = new CustArrayResponse();
                        using (MySqlDataReader ReaderCount = custcommand.ExecuteReader())
                        {
                            if (ReaderCount.Read())
                            {
                                CustomerSearchResponse csr = new CustomerSearchResponse { FirstName = ReaderCount["FirstName"].ToString(), LastName = ReaderCount["LastName"].ToString(), MiddleName = ReaderCount["MiddleName"].ToString(), Street = ReaderCount["Street"].ToString(), ProvinceCity = ReaderCount["ProvinceCity"].ToString(), MLCardNo = ReaderCount["CardNo"].ToString(), BirthDate = Convert.ToDateTime(ReaderCount["Birthdate"].ToString()).ToString("yyyy-MM-dd"), ContactNo = ReaderCount["Mobile"].ToString(), Country = ReaderCount["Country"].ToString(), ExpiryDate = (ReaderCount["ExpiryDate"].Equals(DBNull.Value)) ? ReaderCount["ExpiryDate"].ToString() : Convert.ToDateTime(ReaderCount["ExpiryDate"]).ToString("yyyy-MM-dd"), Gender = ReaderCount["Gender"].ToString(), IDNo = ReaderCount["IDNo"].ToString(), IDType = ReaderCount["IDType"].ToString(), CustID = ReaderCount["CustID"].ToString() };
                                ReaderCount.Close();
                                custconn.Close();
                                kplog.Info("SUCCESS:: message : " + getRespMessage(1) + ", CustomerData: " + csr);
                                return new CustomerResultResponse { respcode = 1, message = getRespMessage(1), CustomerData = csr };
                            }
                            else
                            {
                                kplog.Error("FAILED:: message : " + getRespMessage(5));
                                ReaderCount.Close();
                                custconn.Close();
                                return new CustomerResultResponse { respcode = 5, message = getRespMessage(5) };
                            }
                        }
                    }

                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED: message : " + getRespMessage(0) + ",ErrorDetail: " + ex.ToString());
                    custconn.Close();
                    return new CustomerResultResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), Data = null };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED: message : " + getRespMessage(0) + ",ErrorDetail: " + ex.ToString() + ",Data : " + null);
            custconGlobal.CloseConnection();
            return new CustomerResultResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), Data = null };
        }

    }


    [WebMethod(BufferResponse = false)]
    public CustomerResultResponse customerSearchGlobal(String Username, String Password, String Firstname, String LastName, int page, int perPage, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", Firstname: " + Firstname + ", LastName: " + LastName + ",page : " + page + ", perPage: " + perPage + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Unknown Web Service User!");
            return new CustomerResultResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new CustomerResultResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection custconn = custconGlobal.getConnection())
        {
            try
            {
                custconn.Open();
                using (custcommand = custconn.CreateCommand())
                {
                    int counter = 0;
                    int start;
                    if (page == 1)
                    {
                        start = 0;
                    }
                    else
                    {
                        start = (page - 1) * perPage;
                    }
                    Double totalcount;
                    String finalCount;
                    String countTotPages = "select COUNT(c.custid) as total from kpcustomersglobal.customers c left join kpcustomersglobal.customersdetails cc on c.custid = cc.CustID where c.FirstName like @FirstNamex AND c.LastName like @LastNamex";
                    custcommand.CommandText = countTotPages;

                    custcommand.Parameters.AddWithValue("FirstNamex", Firstname + '%');
                    custcommand.Parameters.AddWithValue("LastNamex", LastName + '%');
                    using (MySqlDataReader ReaderCountTOT = custcommand.ExecuteReader())
                    {
                        ReaderCountTOT.Read();
                        var result = Convert.ToDouble(ReaderCountTOT["total"]) % perPage;
                        if (result == 0)
                        {
                            totalcount = Convert.ToDouble(ReaderCountTOT["total"]) / perPage;
                        }
                        else
                        {
                            totalcount = Convert.ToDouble(ReaderCountTOT["total"]) / perPage + 1;
                        }
                        //totalcount = Convert.ToDouble(ReaderCountTOT["total"]) / perPage;
                        ReaderCountTOT.Close();
                    }
                    finalCount = Math.Truncate(totalcount).ToString();
                    custcommand.Parameters.Clear();
                    //String query = "select c.FirstName,c.LastName,c.MiddleName,c.Street,c.ProvinceCity,c.BirthDate,c.Country,c.ZipCode,c.ExpiryDate,c.Gender,c.IDNo,c.IDType,c.CustID,c.PhoneNo,c.Mobile,c.Email,c.cardno,c.IssuedDate,c.PlaceIssued,cc.SecondIDType,cc.SecondIDNo,cc.SecondPlaceIssued,cc.SecondIssuedDate,cc.SecondExpiryDate,cc.HomeCity,cc.WorkStreet,cc.WorkProvinceCity,cc.WorkCity,cc.WorkCountry,cc.WorkZipCode,cc.Occupation,cc.SSN,cc.SourceOfIncome,cc.Relation,cc.ProofOfFunds from kpcustomersglobal.customers c left join kpcustomersglobal.customersdetails cc on c.custid = cc.CustID where c.FirstName like @FirstNamex AND c.LastName like @LastNamex ORDER BY c.LastName LIMIT @start,@end";
                    String query = "select c.FirstName,c.LastName,c.MiddleName,c.Street,c.ProvinceCity,DATE_FORMAT(c.BirthDate,'%Y-%m-%d') as BirthDate,c.Country,c.ZipCode,DATE_FORMAT(c.ExpiryDate,'%Y-%m-%d') as ExpiryDate,c.Gender,c.IDNo,c.IDType,c.CustID,c.PhoneNo,c.Mobile,c.Email,c.cardno,DATE_FORMAT(c.IssuedDate,'%Y-%m-%d') as IssuedDate,c.PlaceIssued,cc.SecondIDType,cc.SecondIDNo,cc.SecondPlaceIssued,DATE_FORMAT(cc.SecondIssuedDate,'%Y-%m-%d') as SecondIssuedDate,DATE_FORMAT(cc.SecondExpiryDate,'%Y-%m-%d') as SecondExpiryDate,cc.HomeCity,cc.WorkStreet,cc.WorkProvinceCity,cc.WorkCity,cc.WorkCountry,cc.WorkZipCode,cc.Occupation,cc.SSN,cc.SourceOfIncome,cc.Relation,cc.ProofOfFunds from kpcustomersglobal.customers c left join kpcustomersglobal.customersdetails cc on c.custid = cc.CustID where c.FirstName like @FirstNamex AND c.LastName like @LastNamex ORDER BY c.LastName LIMIT @start,@end";
                    custcommand.CommandText = query;
                    //throw new Exception((start).ToString() + " " + perPage.ToString());
                    custcommand.Parameters.AddWithValue("FirstNamex", Firstname + '%');
                    custcommand.Parameters.AddWithValue("LastNamex", LastName + '%');
                    custcommand.Parameters.AddWithValue("start", start);
                    custcommand.Parameters.AddWithValue("end", perPage);
                    //CustArrayResponse b = new CustArrayResponse();

                    using (MySqlDataReader ReaderCount = custcommand.ExecuteReader())
                    {
                        while (ReaderCount.Read())
                        {
                            counter++;
                        }
                        ReaderCount.Close();
                    }
                    if (counter == 0)
                    {
                        kplog.Error(getRespMessage(5));
                        custconn.Close();
                        kplog.Error("FAILED:: messagE : " + getRespMessage(5));
                        return new CustomerResultResponse { respcode = 5, message = getRespMessage(5) };
                    }

                    using (MySqlDataReader Reader = custcommand.ExecuteReader())
                    {

                        CustArrayResponse[] bb = new CustArrayResponse[counter];
                        int x = 0;
                        while (Reader.Read())
                        {
                            //bb[x] = new CustArrayResponse { SearchItem = new CustomerSearchResponse { FirstName = Reader["FirstName"].ToString(), LastName = Reader["LastName"].ToString(), MiddleName = Reader["MiddleName"].ToString(), Street = Reader["Street"].ToString(), ProvinceCity = Reader["ProvinceCity"].ToString(), MLCardNo = Reader["CardNo"].ToString(), BirthDate = (Reader["Birthdate"].Equals(DBNull.Value) || Reader["Birthdate"].Equals("") || Reader["Birthdate"].ToString().StartsWith("0/") || Reader["Birthdate"].ToString().StartsWith("0")) ? String.Empty : Convert.ToDateTime(Reader["Birthdate"]).ToString("yyyy-MM-dd"), Country = Reader["Country"].ToString(), Zipcode = Reader["ZipCode"].ToString(), ExpiryDate = (Reader["ExpiryDate"].Equals(DBNull.Value) || Reader["ExpiryDate"].Equals("") || Reader["ExpiryDate"].ToString().StartsWith("0/") || Reader["ExpiryDate"].ToString().StartsWith("0")) ? String.Empty : Convert.ToDateTime(Reader["ExpiryDate"]).ToString("yyyy-MM-dd"), Gender = Reader["Gender"].ToString(), IDNo = Reader["IDNo"].ToString(), IDType = Reader["IDType"].ToString(), CustID = Reader["CustID"].ToString(), PhoneNo = Reader["PhoneNo"].ToString(), Mobile = Reader["Mobile"].ToString(), Email = Reader["Email"].ToString(), dtissued = (Reader["IssuedDate"].Equals(DBNull.Value) || Reader["IssuedDate"].Equals("") || Reader["IssuedDate"].ToString().StartsWith("0/") || Reader["IssuedDate"].ToString().StartsWith("0")) ? String.Empty : Convert.ToDateTime(Reader["IssuedDate"]).ToString("yyyy-MM-dd"), placeissued = Reader["PlaceIssued"].ToString(), secondidtype = Reader["SecondIDType"].ToString(), secondidno = Reader["SecondIDNo"].ToString(), secondplaceissued = Reader["SecondPlaceIssued"].ToString(), secondissueddate = (Reader["SecondIssuedDate"].Equals(DBNull.Value) || Reader["SecondIssuedDate"].Equals("") || Reader["SecondIssuedDate"].ToString().StartsWith("0/") || Reader["SecondIssuedDate"].ToString().StartsWith("0")) ? String.Empty : Convert.ToDateTime(Reader["SecondIssuedDate"]).ToString("yyyy-MM-dd"), secondexpirydate = (Reader["SecondExpiryDate"].Equals(DBNull.Value) || Reader["SecondExpiryDate"].Equals("") || Reader["SecondExpiryDate"].ToString().StartsWith("0/") || Reader["SecondExpiryDate"].ToString().StartsWith("0")) ? String.Empty : Convert.ToDateTime(Reader["SecondExpiryDate"]).ToString("yyyy-MM-dd"), homecity = Reader["HomeCity"].ToString(), workstreet = Reader["WorkStreet"].ToString(), workprovcity = Reader["WorkProvinceCity"].ToString(), workcity = Reader["WorkCity"].ToString(), workcountry = Reader["WorkCountry"].ToString(), workzipcode = Reader["WorkZipCode"].ToString(), occupation = Reader["Occupation"].ToString(), ssn = Reader["SSN"].ToString(), sourceofincome = Reader["SourceOfIncome"].ToString(), relation = Reader["Relation"].ToString(), proofoffunds = Reader["ProofOfFunds"].ToString() } };
                            bb[x] = new CustArrayResponse { SearchItem = new CustomerSearchResponse { FirstName = Reader["FirstName"].ToString(), LastName = Reader["LastName"].ToString(), MiddleName = Reader["MiddleName"].ToString(), Street = Reader["Street"].ToString(), ProvinceCity = Reader["ProvinceCity"].ToString(), MLCardNo = Reader["CardNo"].ToString(), BirthDate = (Reader["Birthdate"].Equals(DBNull.Value) || Reader["Birthdate"].Equals("") || Reader["BirthDate"].ToString().Equals(String.Empty) || Reader["Birthdate"].ToString().StartsWith("0/") || Reader["Birthdate"].ToString().StartsWith("0")) ? String.Empty : Reader["Birthdate"].ToString(), Country = Reader["Country"].ToString(), Zipcode = Reader["ZipCode"].ToString(), ExpiryDate = (Reader["ExpiryDate"].Equals(DBNull.Value) || Reader["ExpiryDate"].Equals("") || Reader["ExpiryDate"].Equals(String.Empty) || Reader["ExpiryDate"].ToString().StartsWith("0/") || Reader["ExpiryDate"].ToString().StartsWith("0")) ? String.Empty : Reader["ExpiryDate"].ToString(), Gender = Reader["Gender"].ToString(), IDNo = Reader["IDNo"].ToString(), IDType = Reader["IDType"].ToString(), CustID = Reader["CustID"].ToString(), PhoneNo = Reader["PhoneNo"].ToString(), Mobile = Reader["Mobile"].ToString(), Email = Reader["Email"].ToString(), dtissued = (Reader["IssuedDate"].Equals(DBNull.Value) || Reader["IssuedDate"].Equals("") || Reader["IssuedDate"].Equals(String.Empty) || Reader["IssuedDate"].ToString().StartsWith("0/") || Reader["IssuedDate"].ToString().StartsWith("0")) ? String.Empty : Reader["IssuedDate"].ToString(), placeissued = Reader["PlaceIssued"].ToString(), secondidtype = Reader["SecondIDType"].ToString(), secondidno = Reader["SecondIDNo"].ToString(), secondplaceissued = Reader["SecondPlaceIssued"].ToString(), secondissueddate = (Reader["SecondIssuedDate"].Equals(DBNull.Value) || Reader["SecondIssuedDate"].Equals("") || Reader["SecondIssuedDate"].Equals(String.Empty) || Reader["SecondIssuedDate"].ToString().StartsWith("0/") || Reader["SecondIssuedDate"].ToString().StartsWith("0")) ? String.Empty : Reader["SecondIssuedDate"].ToString(), secondexpirydate = (Reader["SecondExpiryDate"].Equals(DBNull.Value) || Reader["SecondExpiryDate"].Equals("") || Reader["SecondExpiryDate"].Equals(String.Empty) || Reader["SecondExpiryDate"].ToString().StartsWith("0/") || Reader["SecondExpiryDate"].ToString().StartsWith("0")) ? String.Empty : Reader["SecondExpiryDate"].ToString(), homecity = Reader["HomeCity"].ToString(), workstreet = Reader["WorkStreet"].ToString(), workprovcity = Reader["WorkProvinceCity"].ToString(), workcity = Reader["WorkCity"].ToString(), workcountry = Reader["WorkCountry"].ToString(), workzipcode = Reader["WorkZipCode"].ToString(), occupation = Reader["Occupation"].ToString(), ssn = Reader["SSN"].ToString(), sourceofincome = Reader["SourceOfIncome"].ToString(), relation = Reader["Relation"].ToString(), proofoffunds = Reader["ProofOfFunds"].ToString() } };
                            x = x + 1;
                        }
                        //throw new Exception(x.ToString());
                        Reader.Close();

                        custconn.Close();
                        kplog.Info("SUCCESS: respcode: 1, message: " + getRespMessage(1) + ", data: " + bb + ", nextPage: " + (page + 1) + ", prevPage: " + (page - 1) + ", totalPages: " + finalCount);
                        return new CustomerResultResponse { respcode = 1, message = getRespMessage(1), Data = bb, nextPage = page + 1, prevPage = page - 1, totalPages = finalCount };
                    }


                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: message : " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", Data: " + null);
                custconn.Close();
                return new CustomerResultResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), Data = null };
            }
        }
    }

    [WebMethod(BufferResponse = false)]
    public CustomerResultResponse customerSearch(String Username, String Password, String Firstname, String LastName, int page, int perPage, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", Firstname: " + Firstname + ", LastName: " + LastName + ",page : " + page + ", perPage: " + perPage + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Unknown Web Service User!");
            return new CustomerResultResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new CustomerResultResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection custconn = custconDomestic.getConnection())
        {
            try
            {
                custconn.Open();
                using (custcommand = custconn.CreateCommand())
                {
                    int counter = 0;
                    int start;
                    if (page == 1)
                    {
                        start = 0;
                    }
                    else
                    {
                        start = (page - 1) * perPage;
                    }
                    Double totalcount;
                    String finalCount;
                    String countTotPages = "select COUNT(custid) as total from kpcustomers.customers c where FirstName = @FirstNamex AND LastName = @LastNamex";
                    custcommand.CommandText = countTotPages;

                    custcommand.Parameters.AddWithValue("FirstNamex", Firstname);
                    custcommand.Parameters.AddWithValue("LastNamex", LastName);
                    using (MySqlDataReader ReaderCountTOT = custcommand.ExecuteReader())
                    {
                        ReaderCountTOT.Read();
                        var result = Convert.ToDouble(ReaderCountTOT["total"]) % perPage;
                        if (result == 0)
                        {
                            totalcount = Convert.ToDouble(ReaderCountTOT["total"]) / perPage;
                        }
                        else
                        {
                            totalcount = Convert.ToDouble(ReaderCountTOT["total"]) / perPage + 1;
                        }
                        //totalcount = Convert.ToDouble(ReaderCountTOT["total"]) / perPage;
                        ReaderCountTOT.Close();
                    }
                    finalCount = Math.Truncate(totalcount).ToString();
                    custcommand.Parameters.Clear();
                    String query = "select FirstName,LastName,MiddleName,Street,ProvinceCity,BirthDate,Country,ExpiryDate,Gender,IDNo,IDType,CustID,PhoneNo,Mobile,Email, cardno  from kpcustomers.customers c where FirstName = @FirstNamex AND LastName = @LastNamex ORDER BY LastName LIMIT @start,@end";
                    custcommand.CommandText = query;
                    //throw new Exception((start).ToString() + " " + perPage.ToString());
                    custcommand.Parameters.AddWithValue("FirstNamex", Firstname);
                    custcommand.Parameters.AddWithValue("LastNamex", LastName);
                    custcommand.Parameters.AddWithValue("start", start);
                    custcommand.Parameters.AddWithValue("end", perPage);
                    //CustArrayResponse b = new CustArrayResponse();

                    using (MySqlDataReader ReaderCount = custcommand.ExecuteReader())
                    {
                        while (ReaderCount.Read())
                        {
                            counter++;
                        }
                        ReaderCount.Close();
                    }
                    if (counter == 0)
                    {
                        kplog.Error("FAILED:: message : " + getRespMessage(5));
                        custconn.Close();
                        return new CustomerResultResponse { respcode = 5, message = getRespMessage(5) };
                    }

                    using (MySqlDataReader Reader = custcommand.ExecuteReader())
                    {

                        CustArrayResponse[] bb = new CustArrayResponse[counter];
                        int x = 0;
                        while (Reader.Read())
                        {

                            bb[x] = new CustArrayResponse { SearchItem = new CustomerSearchResponse { FirstName = Reader["FirstName"].ToString(), LastName = Reader["LastName"].ToString(), MiddleName = Reader["MiddleName"].ToString(), Street = Reader["Street"].ToString(), ProvinceCity = Reader["ProvinceCity"].ToString(), MLCardNo = Reader["CardNo"].ToString(), BirthDate = (Reader["Birthdate"].Equals(DBNull.Value) || Reader["Birthdate"].Equals("") || Reader["Birthdate"].ToString().StartsWith("0/")) ? String.Empty : Convert.ToDateTime(Reader["Birthdate"]).ToString("yyyy-MM-dd"), Country = Reader["Country"].ToString(), ExpiryDate = (Reader["ExpiryDate"].Equals(DBNull.Value) || Reader["ExpiryDate"].Equals("") || Reader["ExpiryDate"].ToString().StartsWith("0/")) ? String.Empty : Convert.ToDateTime(Reader["ExpiryDate"]).ToString("yyyy-MM-dd"), Gender = Reader["Gender"].ToString(), IDNo = Reader["IDNo"].ToString(), IDType = Reader["IDType"].ToString(), CustID = Reader["CustID"].ToString(), PhoneNo = Reader["PhoneNo"].ToString(), Mobile = Reader["Mobile"].ToString(), Email = Reader["Email"].ToString() } };
                            x = x + 1;
                        }
                        //throw new Exception(x.ToString());
                        Reader.Close();

                        custconn.Close();
                        kplog.Info("SUCCESS: respcode: 1, message: " + getRespMessage(1) + ", Data: " + bb + ", nextPage: " + (page + 1) + ", prevPage: " + (page - 1) + ", totalPages: " + finalCount);
                        return new CustomerResultResponse { respcode = 1, message = getRespMessage(1), Data = bb, nextPage = page + 1, prevPage = page - 1, totalPages = finalCount };
                    }


                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: message : " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", Data: " + null);
                custconn.Close();
                return new CustomerResultResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), Data = null };
            }
        }
    }

    //[WebMethod]
    //public AddKYCResponse addKYCGlobal(String Username, String Password, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderBirthdate, String SenderBranchID, String IDType, String IDNo, String ExpiryDate, String PhoneNo, String MobileNo, String Email, String CreatedBy, Double version, String stationcode)
    //{
    //    if (!authenticate(Username, Password))
    //    {
    //        return new AddKYCResponse { respcode = 7, message = getRespMessage(7) };
    //    }
    //    //if (!compareVersions(getVersion(stationcode), version))
    //    //{
    //    //    return new AddKYCResponse { respcode = 10, message = getRespMessage(10) };
    //    //}
    //    //Waiting for further instructions.
    //    //if (verifyCustomer(SenderFName, SenderLName, SenderMName, SenderBirthdate)) {

    //    //    return new AddKYCResponse { respcode = 6, message = getRespMessage(6) };
    //    //}
    //    try
    //    {
    //        dt = getServerDateGlobal(false);
    //    }
    //    catch (Exception ex)
    //    {
    //        kplog.Error("Exception catch");
    //        return new AddKYCResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), MLCardNo = null };
    //    }
    //    using (MySqlConnection custconn = custconGlobal.getConnection())
    //    {
    //        try
    //        {
    //            custconn.Open();

    //            custtrans = custconn.BeginTransaction(IsolationLevel.ReadCommitted);
    //            //using (command = custconn.CreateCommand()) {
    //            //    dt = getServerDate(true);
    //            //}
    //            using (custcommand = custconn.CreateCommand())
    //            {

    //                custcommand.Transaction = custtrans;
    //                string senderid = generateCustIDGlobal(custcommand);
    //                String updatesender = "update kpformsglobal.customerseries set series = series + 1";
    //                custcommand.CommandText = updatesender;
    //                custcommand.ExecuteNonQuery();


    //                if (!SenderMLCardNO.Equals(string.Empty))
    //                {
    //                    //addKYC_insert_cardno proc
    //                    String insertMLCard = "INSERT INTO kpcustomersglobal.customercard (CardNo) VALUES (@CardNo)";
    //                    custcommand.CommandText = insertMLCard;
    //                    custcommand.Parameters.AddWithValue("CardNo", SenderMLCardNO);
    //                    //custcommand.Parameters.AddWithValue("CustID", senderid);
    //                    custcommand.ExecuteNonQuery();
    //                }

    //                //addKYC_insert_customers proc
    //                String insertCustomer = "INSERT INTO kpcustomersglobal.customers (CustID, FirstName, LastName, MiddleName, Street, ProvinceCity, Country, Gender, Birthdate, IDType, IDNo, DTCreated, ExpiryDate, CreatedBy, PhoneNo, Mobile, Email, cardno) VALUES (@SCustID, @SFirstName, @SLastName, @SMiddleName, @SStreet, @SProvinceCity, @SCountry, @SGender, @SBirthdate, @IDType, @IDNo, @DTCreated, @ExpiryDate,@CreatedBy, @PhoneNo, @MobileNo, @Email, @mlcardno);";
    //                custcommand.CommandText = insertCustomer;

    //                custcommand.Parameters.AddWithValue("SCustID", senderid);
    //                //custcommand.Parameters.AddWithValue("SMLCardNo", SenderMLCardNO);
    //                custcommand.Parameters.AddWithValue("SFirstName", SenderFName);
    //                custcommand.Parameters.AddWithValue("SLastName", SenderLName);
    //                custcommand.Parameters.AddWithValue("SMiddleName", SenderMName);
    //                custcommand.Parameters.AddWithValue("SStreet", SenderStreet);
    //                custcommand.Parameters.AddWithValue("SProvinceCity", SenderProvinceCity);
    //                custcommand.Parameters.AddWithValue("SCountry", SenderCountry);
    //                custcommand.Parameters.AddWithValue("SGender", SenderGender);
    //                //custcommand.Parameters.AddWithValue("SContactNo", SenderContactNo);
    //                custcommand.Parameters.AddWithValue("SBirthdate", SenderBirthdate);
    //                //custcommand.Parameters.AddWithValue("SBranchID", SenderBranchID);
    //                custcommand.Parameters.AddWithValue("DTCreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
    //                custcommand.Parameters.AddWithValue("IDType", IDType);
    //                custcommand.Parameters.AddWithValue("IDNo", IDNo);
    //                custcommand.Parameters.AddWithValue("ExpiryDate", (ExpiryDate.Equals(String.Empty)) ? null : ExpiryDate);
    //                custcommand.Parameters.AddWithValue("PhoneNo", PhoneNo);
    //                custcommand.Parameters.AddWithValue("MobileNo", MobileNo);
    //                custcommand.Parameters.AddWithValue("Email", Email);
    //                custcommand.Parameters.AddWithValue("CreatedBy", CreatedBy);
    //                custcommand.Parameters.AddWithValue("mlcardno", SenderMLCardNO);
    //                custcommand.ExecuteNonQuery();




    //                custtrans.Commit();
    //                custconn.Close();
    //                return new AddKYCResponse { respcode = 1, message = getRespMessage(1), MLCardNo = senderid };

    //            }
    //        }
    //        catch (Exception mex)
    //        {
    //            kplog.Fatal(mex.ToString());
    //            custtrans.Rollback();
    //            custconn.Close();
    //            int respcode = 0;
    //            if (mex.Message.StartsWith("Duplicate"))
    //            {
    //                respcode = 6;
    //                kplog.Fatal(getRespMessage(respcode), mex);
    //            }
    //            return new AddKYCResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = mex.ToString() };
    //        }
    //    }

    //}

    [WebMethod]
    public String getsubstr(String creator)
    {
        kplog.Info(" creator: "+creator);

        String createdby = creator.Substring(4);
        return createdby;
    }

    [WebMethod]
    public AddKYCResponse addKYCGlobal(String Username, String Password, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String ZipCode, String SenderGender, String SenderBirthdate, String SenderBranchID, String IDType, String IDNo, String ExpiryDate, String PhoneNo, String MobileNo, String Email, String CreatedBy, Double version, String stationcode, String dtissue, String placeissue, String createdbybranch, String sidtype, String sidno, String splaceissue, String sdtissue, String sexpirydate, String sworkstreet, String sworkprovcity, String sworkcountry, String sworkzipcode, String soccupation, String sssn, String ssourceofincome, String srelation, String sproofoffunds, String shomecity, String sworkcity)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", SenderFName: " + SenderFName + ",SenderLName : " + SenderLName + ", SenderMName: " + SenderMName + ",SenderStreet : " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ",ZipCode : " + ZipCode + ", SenderGender: " + SenderGender + " ,SenderBirthdate :" + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", IDType: " + IDType + ", IDNo: " + IDNo + ", ExpiryDate: " + ExpiryDate +  ", PhoneNo: " + PhoneNo + ", MobileNo: " + MobileNo + ", Email: " + Email + ", version: " + version + ", stationcode: " + stationcode + ", dttissue: " + dtissue + ", placeissue: " + placeissue + ", sidtype: " + sidtype + ", sidno: " + sidno + ", splaceissue: " + splaceissue + ", sdtissue: " + sdtissue + ", sexpirydate: " + sexpirydate + ", sworkstreet: " + sworkstreet + ", sworkprovcity: " + sworkprovcity + ", sworkcountry: " + sworkcountry + ", sworkzipcode: " + sworkzipcode + ", soccupation: " + soccupation + ", sssn: " + sssn + ", ssourceofincome: " + ssourceofincome + ", srelation: " + srelation + ", sproofoffunds: " + sproofoffunds + ", shomecity: " + shomecity + ", sworkcity: " + sworkcity);

        if (!authenticate(Username, Password))
        {
            return new AddKYCResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new AddKYCResponse { respcode = 10, message = getRespMessage(10) };
        //}
        //Waiting for further instructions.
        if (verifyCustomer(SenderFName, SenderLName, SenderMName, SenderBirthdate))
        {
            kplog.Error("FAILED:: message : " + getRespMessage(6));
            return new AddKYCResponse { respcode = 6, message = getRespMessage(6) };
        }
        try
        {
            dt = getServerDateGlobal(false);
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString() + ", MLCardNo: " + null);
            return new AddKYCResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), MLCardNo = null };
        }


        using (MySqlConnection custconn = custconGlobal.getConnection())
        {
            try
            {
                custconn.Open();
                Int32 sr = 0;
                string updatecustomerseries = String.Empty;
                string senderid = String.Empty;
                custtrans = custconn.BeginTransaction(IsolationLevel.ReadCommitted);

                using (custcommand = custconn.CreateCommand())
                {

                    //string senderid = generateCustIDGlobal(custcommand);
                    String query = "select series from kpformsglobal.customerseries";
                    custcommand.CommandText = query;
                    MySqlDataReader Reader = custcommand.ExecuteReader();
                    if (Reader.HasRows)
                    {
                        Reader.Read();
                        if (!(Reader["series"] == DBNull.Value))
                        {
                            sr = Convert.ToInt32(Reader["series"].ToString());
                        }
                    }
                    Reader.Close();

                    Int32 sr1 = sr + 1;
                    custcommand.Transaction = custtrans;

                    if (sr == 0)
                    {
                        updatecustomerseries = "INSERT INTO kpformsglobal.customerseries(series,year) values('" + sr1 + "','" + dt.ToString("yyyy") + "')";
                        kplog.Info("SUCCES:: message: INSERT INTO kpformsglobal.customerseries: series: " + sr1 + ", year" + dt.ToString("yyyy"));
                    }
                    else
                    {
                        updatecustomerseries = "update kpformsglobal.customerseries set series = '" + sr1 + "', year = '" + dt.ToString("yyyy") + "'";
                        kplog.Info("SUCCES:: message : kpformsglobal.customerseries : series: " + sr1 + " , year: " + dt.ToString("yyyy"));
                    }
                    //custcommand.Transaction = custtrans;

                    //String updatesender = "update kpformsglobal.customerseries set series = series + 1";
                    custcommand.CommandText = updatecustomerseries;
                    custcommand.ExecuteNonQuery();

                    if (!SenderMLCardNO.Equals(string.Empty))
                    {
                        //addKYC_insert_cardno proc
                        String insertMLCard = "INSERT INTO kpcustomersglobal.customercard (CardNo) VALUES (@CardNo)";
                        custcommand.CommandText = insertMLCard;
                        custcommand.Parameters.AddWithValue("CardNo", SenderMLCardNO);
                        custcommand.ExecuteNonQuery();
                        kplog.Info("SUCCES:: message: INSERT INTO kpcustomersglobal.customercard : CardNo : " + SenderMLCardNO);
                    }

                    //addKYC_insert_customers proc
                    senderid = generateCustIDGlobal(custcommand);

                    String insertCustomer = "INSERT INTO kpcustomersglobal.customers (CustID, FirstName, LastName, MiddleName, Street, ProvinceCity, Country, ZipCode, Gender, Birthdate, IDType, IDNo, DTCreated, ExpiryDate, CreatedBy, PhoneNo, Mobile, Email, cardno, IssuedDate, CreatedByBranch, PlaceIssued) VALUES (@SCustID, @SFirstName, @SLastName, @SMiddleName, @SStreet, @SProvinceCity, @SCountry, @SZipcode, @SGender, @SBirthdate, @IDType, @IDNo, @DTCreated, @ExpiryDate,@CreatedBy, @PhoneNo, @MobileNo, @Email, @mlcardno, @dtissued, @cbybranch, @placeissued);";
                    custcommand.CommandText = insertCustomer;

                    custcommand.Parameters.Clear();
                    custcommand.Parameters.AddWithValue("SCustID", senderid);
                    custcommand.Parameters.AddWithValue("SFirstName", SenderFName);
                    custcommand.Parameters.AddWithValue("SLastName", SenderLName);
                    custcommand.Parameters.AddWithValue("SMiddleName", SenderMName);
                    custcommand.Parameters.AddWithValue("SStreet", SenderStreet);
                    custcommand.Parameters.AddWithValue("SProvinceCity", SenderProvinceCity);
                    custcommand.Parameters.AddWithValue("SZipcode", ZipCode);
                    custcommand.Parameters.AddWithValue("SCountry", SenderCountry);
                    custcommand.Parameters.AddWithValue("SGender", SenderGender);
                    custcommand.Parameters.AddWithValue("SBirthdate", SenderBirthdate);
                    custcommand.Parameters.AddWithValue("DTCreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    custcommand.Parameters.AddWithValue("IDType", IDType);
                    custcommand.Parameters.AddWithValue("IDNo", IDNo);
                    custcommand.Parameters.AddWithValue("ExpiryDate", (ExpiryDate.Equals(String.Empty)) ? null : ExpiryDate);
                    custcommand.Parameters.AddWithValue("PhoneNo", PhoneNo);
                    custcommand.Parameters.AddWithValue("MobileNo", MobileNo);
                    custcommand.Parameters.AddWithValue("Email", Email);
                    custcommand.Parameters.AddWithValue("CreatedBy", CreatedBy);
                    custcommand.Parameters.AddWithValue("mlcardno", SenderMLCardNO);
                    custcommand.Parameters.AddWithValue("dtissued", (dtissue.Equals(String.Empty)) ? null : dtissue);
                    custcommand.Parameters.AddWithValue("cbybranch", createdbybranch);
                    custcommand.Parameters.AddWithValue("placeissued", placeissue);
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCES:: message : INSERT INTO kpcustomersglobal.customers:  "
                     + " SCustID: " + senderid
                     + " SFirstName: " + SenderFName
                     + " SLastName: " + SenderLName
                     + " SMiddleName: " + SenderMName
                     + " SStreet: " + SenderStreet
                     + " SProvinceCity: " + SenderProvinceCity
                     + " SZipcode: " + ZipCode
                     + " SCountry: " + SenderCountry
                     + " SGender: " + SenderGender
                     + " SBirthdate: " + SenderBirthdate
                     + " DTCreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                     + " IDType: " + IDType
                     + " IDNo: " + IDNo
                     + " ExpiryDate: " + (ExpiryDate == String.Empty ? null : ExpiryDate)
                     + " PhoneNo: " + PhoneNo
                     + " MobileNo: " + MobileNo
                     + " Email: " + Email
                     + " CreatedBy: " + CreatedBy
                     + " mlcardno: " + SenderMLCardNO
                     + " dtissued: " + (dtissue == String.Empty ? null : dtissue)
                     + " cbybranch: " + createdbybranch
                     + " placeissued: " + placeissue);

                    String insertCustomerDetails = "INSERT INTO kpcustomersglobal.customersdetails(CustID,SecondIDType,SecondIDNo,SecondPlaceIssued,SecondIssuedDate,SecondExpiryDate,HomeCity,WorkStreet,WorkProvinceCity,WorkCity,WorkCountry,WorkZipCode,Occupation,SSN,SourceOfIncome,Relation,ProofOfFunds) values(@dcustid,@dsidtype,@dsidno,@dsplaceissued,@dsdateissued,@dsexpirydate,@dhomecity,@dworkstreet,@dworkprovincecity,@dworkcity,@dworkcountry,@dworkzipcode,@doccupation,@dssn,@dsourceofincome,@drelation,@dproofoffunds)";
                    custcommand.CommandText = insertCustomerDetails;

                    custcommand.Parameters.Clear();
                    custcommand.Parameters.AddWithValue("dcustid", senderid);
                    custcommand.Parameters.AddWithValue("dsidtype", sidtype);
                    custcommand.Parameters.AddWithValue("dsidno", sidno);
                    custcommand.Parameters.AddWithValue("dsplaceissued", splaceissue);
                    custcommand.Parameters.AddWithValue("dsdateissued", (sdtissue.Equals(String.Empty)) ? null : sdtissue);
                    custcommand.Parameters.AddWithValue("dsexpirydate", (sexpirydate.Equals(String.Empty)) ? null : sexpirydate);
                    custcommand.Parameters.AddWithValue("dhomecity", shomecity);
                    custcommand.Parameters.AddWithValue("dworkstreet", sworkstreet);
                    custcommand.Parameters.AddWithValue("dworkprovincecity", sworkprovcity);
                    custcommand.Parameters.AddWithValue("dworkcity", sworkcity);
                    custcommand.Parameters.AddWithValue("dworkcountry", sworkcountry);
                    custcommand.Parameters.AddWithValue("dworkzipcode", sworkzipcode);
                    custcommand.Parameters.AddWithValue("doccupation", soccupation);
                    custcommand.Parameters.AddWithValue("dssn", sssn);
                    custcommand.Parameters.AddWithValue("dsourceofincome", ssourceofincome);
                    custcommand.Parameters.AddWithValue("drelation", srelation);
                    custcommand.Parameters.AddWithValue("dproofoffunds", sproofoffunds);
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCESS: message : INSERT INTO kpcustomersglobal.customersdetails: "
                    + " dcustid: " + senderid
                    + " dsidtype: " + sidtype
                    + " dsidno: " + sidno
                    + " dsplaceissued: " + splaceissue
                    + " dsdateissued: " + (sdtissue == String.Empty ? null : sdtissue)
                    + " dsexpirydate: " + (sexpirydate == String.Empty ? null : sexpirydate)
                    + " dhomecity: " + shomecity
                    + " dworkstreet: " + sworkstreet
                    + " dworkprovincecity: " + sworkprovcity
                    + " dworkcity: " + sworkcity
                    + " dworkcountry: " + sworkcountry
                    + " dworkzipcode: " + sworkzipcode
                    + " doccupation: " + soccupation
                    + " dssn: " + sssn
                    + " dsourceofincome: " + ssourceofincome
                    + " drelation: " + srelation
                    + " dproofoffunds: " + sproofoffunds);

                    //String insertCustomerRoutingAccount = "INSERT INTO kpcustomersglobal.customers_routing_account(scustid,saccountno,sroutingno,syscreated) values(@vscustid,@vsaccountno,@vsroutingno,now())";
                    //custcommand.CommandText = insertCustomerRoutingAccount;
                    //custcommand.Parameters.Clear();
                    //custcommand.Parameters.AddWithValue("vscustid", senderid);
                    //custcommand.Parameters.AddWithValue("vsaccountno", saccountno);
                    //custcommand.Parameters.AddWithValue("vsroutingno", sroutingno);
                    //custcommand.ExecuteNonQuery();

                    String creator = CreatedBy.Substring(4);
                    String insertCustomerLogs = "INSERT INTO kpadminlogsglobal.customerlogs(ScustID,Details,Syscreated,Syscreator,Type) values(@scustid,@details,now(),@creator,@type)";
                    custcommand.CommandText = insertCustomerLogs;
                    custcommand.Parameters.Clear();
                    custcommand.Parameters.AddWithValue("scustid", senderid);
                    custcommand.Parameters.AddWithValue("details", "{Name:" + " " + SenderFName + " " + SenderMName + " " + SenderLName + ", " + "Street:" + " " + SenderStreet + ", " + "ProvinceCity:" + " " + SenderProvinceCity + ", " + "ZipCode:" + " " + ZipCode + ", " + "Country:" + " " + SenderCountry + ", " + "Gender:" + " " + SenderGender + ", " + "BirthDate:" + " " + SenderBirthdate + ", " + "IDType:" + " " + IDType + ", " + "IDNo:" + " " + IDNo + ", " + "ExpiryDate:" + " " + ExpiryDate + ", " + "PhoneNo:" + " " + PhoneNo + ", " + "MobileNo:" + " " + MobileNo + ", " + "Email:" + " " + Email + ", " + "MlcardNo:" + " " + SenderMLCardNO + ", " + "IssuedDate:" + " " + ((dtissue.Equals(String.Empty)) ? null : dtissue) + ", " + "CreatedByBranch" + " " + createdbybranch + ", " + "PlaceIssued:" + " " + placeissue + "}");
                    custcommand.Parameters.AddWithValue("creator", creator);
                    custcommand.Parameters.AddWithValue("type", "N");
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCESS: message : INSERT INTO kpadminlogsglobal.customerlogs: "
                    + " scustid: " + senderid
                    + " details: " + "{Name:" + " " + SenderFName + " " + SenderMName + " " + SenderLName + "  " + "Street:" + " " + SenderStreet + "  " + "ProvinceCity:" + " " + SenderProvinceCity + "  " + "ZipCode:" + " " + ZipCode + "  " + "Country:" + " " + SenderCountry + "  " + "Gender:" + " " + SenderGender + "  " + "BirthDate:" + " " + SenderBirthdate + "  " + "IDType:" + " " + IDType + "  " + "IDNo:" + " " + IDNo + "  " + "ExpiryDate:" + " " + ExpiryDate + "  " + "PhoneNo:" + " " + PhoneNo + "  " + "MobileNo:" + " " + MobileNo + "  " + "Email:" + " " + Email + "  " + "MlcardNo:" + " " + SenderMLCardNO + "  " + "IssuedDate:" + " " + ((dtissue.Equals(String.Empty)) ? null : dtissue) + "  " + "CreatedByBranch" + " " + createdbybranch + "  " + "PlaceIssued:" + " " + placeissue + "}"
                    + " creator: " + creator
                    + " type: " + "N");

                    custtrans.Commit();
                    custconn.Close();
                    kplog.Info("SUCCESS: message : " + getRespMessage(1) + ", MLCardNo: " + senderid);
                    return new AddKYCResponse { respcode = 1, message = getRespMessage(1), MLCardNo = senderid };

                }
            }
            catch (Exception mex)
            {
                kplog.Fatal(mex.ToString());
                custtrans.Rollback();
                custconn.Close();
                int respcode = 0;
                if (mex.Message.StartsWith("Duplicate"))
                {
                    respcode = 6;
                    kplog.Fatal("FAILED: message : " + getRespMessage(respcode) + ", ErrorDetail: " + mex.ToString());
                }
                kplog.Fatal("FAILED:: message : " + getRespMessage(0) + ",ErrorDetail:  " + mex.ToString());
                return new AddKYCResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = mex.ToString() };
            }
        }

    }

    [WebMethod]
    public AddKYCResponse addKYC(String Username, String Password, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderBirthdate, String SenderBranchID, String IDType, String IDNo, String ExpiryDate, String PhoneNo, String MobileNo, String Email, String CreatedBy, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", SenderFName: " + SenderFName + ",SenderLName : " + SenderLName + ", SenderMName: " + SenderMName + ",SenderStreet : " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ", SenderGender: " + SenderGender + " ,SenderBirthdate :" + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", IDType: " + IDType + ", IDNo: " + IDNo + ", ExpiryDate: " + ExpiryDate + ", PhoneNo: " + PhoneNo + ", MobileNo: " + MobileNo + ", Email: " + Email + ", version: " + version + ", stationcode: " + stationcode + ", CreatedBy: "+CreatedBy );

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Unknown Web Service User!");
            return new AddKYCResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new AddKYCResponse { respcode = 10, message = getRespMessage(10) };
        //}
        //Waiting for further instructions.
        //if (verifyCustomer(SenderFName, SenderLName, SenderMName, SenderBirthdate)) {

        //    return new AddKYCResponse { respcode = 6, message = getRespMessage(6) };
        //}
        try
        {
            dt = getServerDateDomestic(false);
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString() + ", MLCardNo: " + null);
            return new AddKYCResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), MLCardNo = null };
        }
        using (MySqlConnection custconn = custconDomestic.getConnection())
        {
            try
            {
                custconn.Open();

                custtrans = custconn.BeginTransaction(IsolationLevel.ReadCommitted);
                //using (command = custconn.CreateCommand()) {
                //    dt = getServerDate(true);
                //}
                using (custcommand = custconn.CreateCommand())
                {

                    custcommand.Transaction = custtrans;
                    string senderid = generateCustID(custcommand);
                    String updatesender = "update kpforms.customerseries set series = series + 1";
                    custcommand.CommandText = updatesender;
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCES:: message : update kpforms.customerseries: series: series + 1 ");


                    if (!SenderMLCardNO.Equals(string.Empty))
                    {
                        //addKYC_insert_cardno proc
                        String insertMLCard = "INSERT INTO kpcustomers.customercard (CardNo) VALUES (@CardNo)";
                        custcommand.CommandText = insertMLCard;
                        custcommand.Parameters.AddWithValue("CardNo", SenderMLCardNO);
                        //custcommand.Parameters.AddWithValue("CustID", senderid);
                        custcommand.ExecuteNonQuery();
                        kplog.Info("SUCCESS: message : INSERT INTO kpcustomers.customercard: CardNo: " + SenderMLCardNO);
                    }


                    String insertCustomer = "INSERT INTO kpcustomers.customers (CustID, FirstName, LastName, MiddleName, Street, ProvinceCity, Country, Gender, Birthdate, IDType, IDNo, DTCreated, ExpiryDate, CreatedBy, PhoneNo, Mobile, Email, cardno) VALUES (@SCustID, @SFirstName, @SLastName, @SMiddleName, @SStreet, @SProvinceCity, @SCountry, @SGender, @SBirthdate, @IDType, @IDNo, @DTCreated, @ExpiryDate,@CreatedBy, @PhoneNo, @MobileNo, @Email, @mlcardno);";
                    custcommand.CommandText = insertCustomer;

                    custcommand.Parameters.AddWithValue("SCustID", senderid);
                    //custcommand.Parameters.AddWithValue("SMLCardNo", SenderMLCardNO);
                    custcommand.Parameters.AddWithValue("SFirstName", SenderFName);
                    custcommand.Parameters.AddWithValue("SLastName", SenderLName);
                    custcommand.Parameters.AddWithValue("SMiddleName", SenderMName);
                    custcommand.Parameters.AddWithValue("SStreet", SenderStreet);
                    custcommand.Parameters.AddWithValue("SProvinceCity", SenderProvinceCity);
                    custcommand.Parameters.AddWithValue("SCountry", SenderCountry);
                    custcommand.Parameters.AddWithValue("SGender", SenderGender);
                    //custcommand.Parameters.AddWithValue("SContactNo", SenderContactNo);
                    custcommand.Parameters.AddWithValue("SBirthdate", SenderBirthdate);
                    //custcommand.Parameters.AddWithValue("SBranchID", SenderBranchID);
                    custcommand.Parameters.AddWithValue("DTCreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    custcommand.Parameters.AddWithValue("IDType", IDType);
                    custcommand.Parameters.AddWithValue("IDNo", IDNo);
                    custcommand.Parameters.AddWithValue("ExpiryDate", (ExpiryDate.Equals(String.Empty)) ? null : ExpiryDate);
                    custcommand.Parameters.AddWithValue("PhoneNo", PhoneNo);
                    custcommand.Parameters.AddWithValue("MobileNo", MobileNo);
                    custcommand.Parameters.AddWithValue("Email", Email);
                    custcommand.Parameters.AddWithValue("CreatedBy", CreatedBy);
                    custcommand.Parameters.AddWithValue("mlcardno", SenderMLCardNO);
                    custcommand.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message : INSERT INTO kpcustomers.customers:  "
                    + " SCustID: " + senderid
                    + " SFirstName: " + SenderFName
                    + " SLastName: " + SenderLName
                    + " SMiddleName: " + SenderMName
                    + " SStreet: " + SenderStreet
                    + " SProvinceCity: " + SenderProvinceCity
                    + " SCountry: " + SenderCountry
                    + " SGender: " + SenderGender
                    + " SBirthdate: " + SenderBirthdate
                    + " DTCreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                    + " IDType: " + IDType
                    + " IDNo: " + IDNo
                    + " ExpiryDate: " + (ExpiryDate == String.Empty ? null : ExpiryDate)
                    + " PhoneNo: " + PhoneNo
                    + " MobileNo: " + MobileNo
                    + " Email: " + Email
                    + " CreatedBy: " + CreatedBy
                    + " mlcardno: " + SenderMLCardNO);




                    custtrans.Commit();
                    custconn.Close();
                    kplog.Info("SUCCESS: message : " + getRespMessage(1) + ", MLCardNo: " + senderid);
                    return new AddKYCResponse { respcode = 1, message = getRespMessage(1), MLCardNo = senderid };

                }
            }
            catch (Exception mex)
            {
              
                custtrans.Rollback();
                custconn.Close();
                int respcode = 0;
                if (mex.Message.StartsWith("Duplicate"))
                {
                    respcode = 6;
                    kplog.Fatal("FAILED:: message : " + getRespMessage(respcode), mex);
                }
                kplog.Fatal("FAILED:: message : " + getRespMessage(respcode) + ", ErrorDetail: " + mex.ToString());
                return new AddKYCResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = mex.ToString() };
            }
        }

    }

    [WebMethod(BufferResponse = false)]
    public PayoutResponse payoutGlobalMobile(String Username, String Password, String ControlNo, String KPTNNo, String OperatorID, String Station, int IsRemote, String RemoteBranch, String RemoteOperatorID, String Reason, String SOBranch, String SOControlNo, String SOOperator, String Currency, Double Principal, String SenderID, String ReceiverID, String Relation, String IDType, String IDNo, String ExpiryDate, String SODate, int sysmodifier, String BranchCode, String series, String ZoneCode, int type, Double balance, Double DormantCharge, String senderid, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, String ORNo, Double ServiceCharge, Double version, String stationcode, int remotezone, String RemoteBranchCode, String POBranchName, double vat, String syscreator, string preferredcurrency, double amountpo, double exchangerate, String transtype)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", ControlNo: " + ControlNo + ",KPTNNo : " + KPTNNo + ", OperatorID: " + OperatorID + ",Station : " + Station + ", IsRemote: " + IsRemote + ", RemoteBranch: " + RemoteBranch + ",RemoteOperatorID : " + RemoteOperatorID + ", Reason: " + Reason + " ,SOBranch :" + SOBranch + ", SOControlNo: " + SOControlNo + ", SOOperator: " + SOOperator + ", Currency: " + Currency + ", Principal: " + Principal + ", SenderID: "+SenderID+", RecieverID: "+ ReceiverID+", Relation: "+Relation+", IDType: "+IDType+", IDNo: "+IDNo+", ExpiryDate: "+ExpiryDate+", SODate: "+SODate+", sysmodifier: "+sysmodifier+", BranchCode: "+BranchCode+", series: "+series+", ZoneCode: "+ZoneCode+", type: "+type+", balance: "+balance+", DormantCharge: "+DormantCharge+", senderid: "+senderid+", SenderMLCardNo: "+SenderMLCardNO+", SenderFName: "+SenderFName+", SenderLName: "+SenderLName+", SenderMName: "+SenderMName+", SenderStreet: "+SenderStreet+", SenderProvinceCity: "+SenderProvinceCity+", SenderCountry: "+SenderCountry+", SenderGender: "+SenderGender+", SenderContactNo: "+SenderContactNo+", SenderIsSMS: "+SenderIsSMS+", SenderBirthdate: "+SenderBirthdate+", SenderBranchID: "+SenderBranchID+", RecieverMLCardNO: "+ReceiverMLCardNO+", RecieverFName: "+ReceiverFName+", RecieverLName: "+ReceiverLName+", RecieverMName: "+ReceiverMName+", RecieverStreet: "+ReceiverStreet+", RecieverProvinceCity: "+ReceiverProvinceCity+", RecieverCountry: "+ReceiverCountry+", RecieverGender: "+ReceiverGender+", RecieverContactNo: "+ReceiverContactNo+", RecieverBirthdate: "+ReceiverBirthdate+", ORNo: "+ORNo+", ServiceCharge: "+ServiceCharge+", version: "+version+", stationcode: "+stationcode+", remotezone: "+remotezone+", RemoteBranchCode: "+RemoteBranchCode+", POBranchName: "+POBranchName+", vat: "+vat+", syscreator: "+syscreator+", preferredcurrency: "+preferredcurrency+", amountpo: "+amountpo+", exchangerate: "+exchangerate+", transtype: "+transtype );

        try
        {
            if (Station.ToString().Equals("0"))
            {
                kplog.Fatal("FAILED:: message :" + getRespMessage(13));
                return new PayoutResponse { respcode = 10, message = getRespMessage(13) };
            }
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Unknown Web Service User!");
                return new PayoutResponse { respcode = 7, message = getRespMessage(7) };
            }

            int sr = ConvertSeries(series);
            dt = getServerDateGlobal(false);

            using (MySqlConnection checkinglang = dbconGlobal.getConnection())
            {
                checkinglang.Open();

                try
                {
                    using (command = checkinglang.CreateCommand())
                    {
                        string checkifcontrolexist = "select controlno from " + generateTableNameGlobalMobile(1, null) + " where controlno=@controlno";
                        command.CommandTimeout = 0;
                        command.CommandText = checkifcontrolexist;
                        command.Parameters.AddWithValue("controlno", ControlNo);
                        MySqlDataReader controlexistreader = command.ExecuteReader();
                        if (controlexistreader.HasRows)
                        {
                            controlexistreader.Close();
                            string query101 = string.Empty;
                            if (IsRemote == 1)
                                query101 = "select max(substring(controlno,length(controlno)-5,length(controlno))) as max1 from " + generateTableNameGlobalMobile(1, null) + " where remotebranch = @branchcode and stationid = @stationid and remotezonecode =@zonecode";
                            else
                                query101 = "select max(substring(controlno,length(controlno)-5,length(controlno))) as max1 from " + generateTableNameGlobalMobile(1, null) + " where branchcode = @branchcode and stationid = @stationid and zonecode=@zonecode";
                            command.CommandText = query101;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("branchcode", IsRemote == 1 ? RemoteBranch : BranchCode);
                            command.Parameters.AddWithValue("stationid", Station);
                            command.Parameters.AddWithValue("zonecode", remotezone == 0 ? Convert.ToInt32(ZoneCode) : remotezone);

                            MySqlDataReader controlmaxreader = command.ExecuteReader();
                            if (controlmaxreader.Read())
                            {
                                sr = Convert.ToInt32(controlmaxreader["max"].ToString()) + 1;

                            }
                            controlmaxreader.Close();

                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("st", IsRemote == 1 ? "00" : Station);
                            command.Parameters.AddWithValue("bcode", IsRemote == 1 ? RemoteBranch : BranchCode);
                            command.Parameters.AddWithValue("series", sr);
                            command.Parameters.AddWithValue("zcode", remotezone == 0 ? Convert.ToInt32(ZoneCode) : remotezone);
                            command.Parameters.AddWithValue("tp", type);
                            int abc101 = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: Update kpformsglobal.control: nseries: " + sr + ", bcode: " + (IsRemote == 1 ? RemoteBranch : BranchCode) + ", station: " + (IsRemote == 1 ? "00" : Station) + ", zcode: " + (remotezone == 0 ? Convert.ToInt32(ZoneCode) : remotezone));

                            checkinglang.Close();
                            dbconGlobal.CloseConnection();
                            kplog.Error("FAILED:: message : Error in saving transaction:: respcode: 101 message: Problem saving transaction. Please close the sendout payout window and try again.");
                            return new PayoutResponse { respcode = 101, message = "Problem saving transaction. Please close the sendout payout window and try again." };

                        }
                    }
                }
                catch (Exception ex)
                {
                    checkinglang.Close();
                    dbconGlobal.CloseConnection();
                    kplog.Error("FAILED:: message : Error in saving transaction:: respcode: 101 message: Problem saving transaction. Please close the sendout payout window and try again.");
                    return new PayoutResponse { respcode = 101, message = "Problem saving transaction. Please close the sendout payout window and try again." };
                }
                checkinglang.Close();
                dbconGlobal.CloseConnection();
            }

            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                try
                {

                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit = 0;";
                        command.ExecuteNonQuery();
                        command.Transaction = trans;
                        String sBdate = (SenderBirthdate == String.Empty) ? null : Convert.ToDateTime(SenderBirthdate).ToString("yyyy-MM-dd");
                        String rBdate = (ReceiverBirthdate == String.Empty) ? null : Convert.ToDateTime(ReceiverBirthdate).ToString("yyyy-MM-dd");
                        String xPiry = (ExpiryDate == String.Empty) ? null : Convert.ToDateTime(ExpiryDate).ToString("yyyy-MM-dd");
                        String month = dt.ToString("MM");
                        String tblorig = "payout" + dt.ToString("MM") + dt.ToString("dd");
                        String insert = "Insert into " + generateTableNameGlobalMobile(1, null) + " (ControlNo, KPTNNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, SOBranch, SOControlNo, SOOperator,  Currency, Principal, Relation, IDType, IDNo, ExpiryDate, ClaimedDate, SODate, syscreated, BranchCode, ZoneCode, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS, SenderBirthdate, SenderBranchID, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, SOORNo, ServiceCharge, RemoteZoneCode,syscreator,preferredpo,amountpo,exchangerate,TransType) values (@ControlNo, @KPTNNo, @OperatorID, @StationID, @IsRemote, @RemoteBranch, @RemoteOperatorID, @Reason, @SOBranch, @SOControlNo, @SOOperator, @Currency, @Principal, @Relation, @IDType, @IDNo, @ExpiryDate, @ClaimedDate, @SODate, @syscreated, @BranchCode, @ZoneCode, @SenderMLCardNO, @SenderFName, @SenderLName, @SenderMName, @SenderName, @SenderStreet, @SenderProvinceCity, @SenderCountry, @SenderGender, @SenderContactNo, @SenderIsSMS, @SenderBirthdate, @SenderBranchID, @ReceiverFName, @ReceiverLName, @ReceiverMName, @ReceiverName, @ReceiverStreet, @ReceiverProvinceCity, @ReceiverCountry, @ReceiverGender, @ReceiverContactNo, @ReceiverBirthdate, @SOORNo, @ServiceCharge, @remotezone, @syscreator,@preferredpo,@amountpo,@exchangerate1,@transtp)";
                        command.CommandText = insert;

                        command.Parameters.AddWithValue("ControlNo", ControlNo);
                        command.Parameters.AddWithValue("KPTNNo", KPTNNo);
                        command.Parameters.AddWithValue("OperatorID", OperatorID);
                        command.Parameters.AddWithValue("StationID", Station);
                        command.Parameters.AddWithValue("IsRemote", IsRemote);
                        command.Parameters.AddWithValue("RemoteBranch", RemoteBranch);
                        command.Parameters.AddWithValue("RemoteOperatorID", RemoteOperatorID);
                        command.Parameters.AddWithValue("Reason", Reason);
                        command.Parameters.AddWithValue("SOBranch", SOBranch);
                        command.Parameters.AddWithValue("SOControlNo", SOControlNo);
                        command.Parameters.AddWithValue("SOOperator", SOOperator);
                        command.Parameters.AddWithValue("Currency", Currency);
                        command.Parameters.AddWithValue("Principal", Principal);
                        command.Parameters.AddWithValue("Relation", Relation);
                        command.Parameters.AddWithValue("IDType", IDType);
                        command.Parameters.AddWithValue("IDNo", IDNo);
                        command.Parameters.AddWithValue("ExpiryDate", xPiry);
                        command.Parameters.AddWithValue("ClaimedDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("SODate", Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreator", syscreator);
                        command.Parameters.AddWithValue("BranchCode", BranchCode);
                        command.Parameters.AddWithValue("ZoneCode", ZoneCode);
                        command.Parameters.AddWithValue("Balance", balance);
                        command.Parameters.AddWithValue("DormantCharge", DormantCharge);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", sBdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", rBdate);
                        command.Parameters.AddWithValue("SOORNo", ORNo);
                        command.Parameters.AddWithValue("ServiceCharge", ServiceCharge);
                        command.Parameters.AddWithValue("remotezone", remotezone);
                        command.Parameters.AddWithValue("vat", vat);
                        command.Parameters.AddWithValue("preferredpo", preferredcurrency);
                        command.Parameters.AddWithValue("amountpo", amountpo);
                        command.Parameters.AddWithValue("exchangerate1", exchangerate);
                        command.Parameters.AddWithValue("transtp", transtype);
                        int x = command.ExecuteNonQuery();

                        kplog.Info("SUCCESS::  message : INSERT INTO " + generateTableNameGlobalMobile(1, null) + ":: ControlNo: " + ControlNo + " KPTNNo: " + KPTNNo + " OperatorID: " + OperatorID + " StationID: " + Station + " IsRemote: " + IsRemote + " RemoteBranch: " + RemoteBranch + " RemoteOperatorID: " + RemoteOperatorID + " Reason: " + Reason + " SOBranch: " + SOBranch + " SOControlNo: " + SOControlNo + " SOOperator: " + SOOperator + " Currency: " + Currency + " Principal: " + Principal + " Relation: " + Relation + " IDType: " + IDType + " IDNo: " + IDNo + " ExpiryDate: " + xPiry + " ClaimedDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " SODate: " + SODate + " syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " BranchCode: " + BranchCode + " ZoneCode: " + ZoneCode + " Balance: " + balance + " DormantCharge: " + DormantCharge + " SenderMLCardNO: " + SenderMLCardNO + " SenderFName: " + SenderFName + " SenderLName: " + SenderLName + " SenderMName: " + SenderMName + " SenderName: " + SenderLName + ", " + SenderFName + " " + SenderMName + " SenderStreet: " + SenderStreet + " SenderProvinceCity: " + SenderProvinceCity + " SenderCountry: " + SenderCountry + " SenderGender: " + SenderGender + " SenderContactNo: " + SenderContactNo + " SenderIsSMS: " + SenderIsSMS + " SenderBirthDate: " + SenderBirthdate + " SenderBranchID: " + SenderBranchID + " ReceiverFName: " + ReceiverFName + " ReceiverLName: " + ReceiverMName + " ReceiverName: " + ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName + " ReceiverStreet: " + ReceiverStreet + " ReceiverProvinceCity: " + ReceiverProvinceCity + " ReceiverCountry: " + ReceiverCountry + " ReceiverGender: " + ReceiverGender + " ReceiverContactNo: " + ReceiverContactNo + " ReceiverBirthDate: " + rBdate + " SOORNo: " + ORNo + " ServiceCharge: " + ServiceCharge + " remotezone: " + remotezone + " vat: " + vat + " preferredpo: " + preferredcurrency + " amountpo: " + amountpo + " exchangerate1: " + exchangerate + " transtp: " + transtype);

                        command.Parameters.Clear();
                        command.CommandText = "update Mobex.sendout" + KPTNNo.Substring(19, 2) + " set batchstatType = 4 where kptnnomobile = @vkptn";
                        command.Parameters.AddWithValue("vkptn", KPTNNo);
                        int zz = command.ExecuteNonQuery();

                        kplog.Info("SUCCESS:: message : UPDATE Mobex.sendout" + KPTNNo.Substring(19, 2) + ":: batchstatType: 4 where kptnnomobile: " + KPTNNo);

                        command.CommandText = "update " + decodeKPTNGlobalMobile(0, KPTNNo) + " set IsClaimed = 1, sysmodified = @modified, sysmodifier = @modifier where KPTNNo = @kptn";
                        command.Parameters.AddWithValue("modified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("modifier", sysmodifier);
                        command.Parameters.AddWithValue("kptn", KPTNNo);
                        int y = command.ExecuteNonQuery();

                        kplog.Info("SUCCESS:: message : UPDATE " + decodeKPTNGlobalMobile(0, KPTNNo) + ":: IsClaimed: 1 sysmodified: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " sysmodifier: " + sysmodifier + " WHERE kptn: " + KPTNNo);

                        if (IsRemote == 1)
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "01");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);//nseries
                            command.Parameters.AddWithValue("zcode", remotezone);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();

                            kplog.Info("SUCCESS:: message : UPDATE control:: nseries: " + sr + 1 + " where bcode: " + RemoteBranch + " station: 01 zcode: " + remotezone + " type: " + type);
                        }
                        else
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", Station);
                            command.Parameters.AddWithValue("bcode", BranchCode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", ZoneCode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();

                            kplog.Info("SUCCESS:: message : UPDATE control:: nseries: " + sr + 1 + " where bcode: " + BranchCode + " station: " + Station + " zcode: " + ZoneCode + " type: " + type);
                        }

                        String insertpayout = "INSERT INTO Mobextransactionsglobal.payout" + month + " (controlno,kptnno,claimeddate," +
                        "operatorid,stationid,isremote,remotebranch,remoteoperatorid,reason,sobranch,socontrolno,sooperator," +
                        "sodate,soorno,syscreated,syscreator,currency,principal,relation,idtype,idno,expirydate,branchcode," +
                        "zonecode,sendermlcardno,senderfname,senderlname,sendermname,sendername,senderstreet," +
                        "senderprovincecity,sendercountry,sendergender,sendercontactno,senderissms,senderbirthdate," +
                        "senderbranchid,receiverfname,receiverlname,receivermname,receivername,receiverstreet," +
                        "receiverprovincecity,receivercountry,receivergender,receivercontactno,receiverbirthdate," +
                        "balance,dormantcharge,servicecharge,vat,remotezonecode,tableoriginated,`year`,pocurrency,poamount,exchangerate,TransType) " +
                        "values (@controlnoP,@kptnnoP,@claimeddateP," +
                        "@operatoridP,@stationidP,@isremoteP,@remotebranchP,@remoteoperatoridP,@reasonP,@sobranchP,@socontrolnoP,@sooperatorP," +
                        "@sodateP,@soornoP,@syscreatedP,@syscreatorP,@currencyP,@principalP,@relationP,@idtypeP,@idnoP,@expirydateP,@branchcodeP," +
                        "@zonecodeP,@sendermlcardnoP,@senderfnameP,@senderlnameP,@sendermnameP,@sendernameP,@senderstreetP," +
                        "@senderprovincecityP,@sendercountryP,@sendergenderP,@sendercontactnoP,@senderissmsP,@senderbirthdateP," +
                        "@senderbranchidP,@receiverfnameP,@receiverlnameP,@receivermnameP,@receivernameP,@receiverstreetP," +
                        "@receiverprovincecityP,@receivercountryP,@receivergenderP,@receivercontactnoP,@receiverbirthdateP," +
                        "@balanceP,@dormantchargeP,@servicechargeP,@vatP,@remotezonecodeP,@tableoriginatedP,@yearP,@pocurrency,@poamount,@exchangerate,@trxntp)";
                        command.CommandText = insertpayout;

                        command.Parameters.AddWithValue("controlnoP", ControlNo);
                        command.Parameters.AddWithValue("kptnnoP", KPTNNo);
                        command.Parameters.AddWithValue("claimeddateP", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("operatoridP", OperatorID);
                        command.Parameters.AddWithValue("stationidP", Station);
                        command.Parameters.AddWithValue("isremoteP", IsRemote);
                        command.Parameters.AddWithValue("remotebranchP", RemoteBranch);
                        command.Parameters.AddWithValue("remoteoperatoridP", RemoteOperatorID);
                        command.Parameters.AddWithValue("reasonP", Reason);
                        command.Parameters.AddWithValue("sobranchP", SOBranch);
                        command.Parameters.AddWithValue("socontrolnoP", SOControlNo);
                        command.Parameters.AddWithValue("sooperatorP", SOOperator);
                        command.Parameters.AddWithValue("sodateP", Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("soornoP", ORNo);
                        command.Parameters.AddWithValue("syscreatedP", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreatorP", syscreator);
                        command.Parameters.AddWithValue("currencyP", Currency);
                        command.Parameters.AddWithValue("principalP", Principal);
                        command.Parameters.AddWithValue("relationP", Relation);
                        command.Parameters.AddWithValue("idtypeP", IDType);
                        command.Parameters.AddWithValue("idnoP", IDNo);
                        command.Parameters.AddWithValue("expirydateP", xPiry);
                        command.Parameters.AddWithValue("branchcodeP", BranchCode);
                        command.Parameters.AddWithValue("zonecodeP", ZoneCode);
                        command.Parameters.AddWithValue("sendermlcardnoP", SenderMLCardNO);
                        command.Parameters.AddWithValue("senderfnameP", SenderFName);
                        command.Parameters.AddWithValue("senderlnameP", SenderLName);
                        command.Parameters.AddWithValue("sendermnameP", SenderMName);
                        command.Parameters.AddWithValue("sendernameP", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("senderstreetP", SenderStreet);
                        command.Parameters.AddWithValue("senderprovincecityP", SenderProvinceCity);
                        command.Parameters.AddWithValue("sendercountryP", SenderCountry);
                        command.Parameters.AddWithValue("sendergenderP", SenderGender);
                        command.Parameters.AddWithValue("sendercontactnoP", SenderContactNo);
                        command.Parameters.AddWithValue("senderissmsP", SenderIsSMS);
                        command.Parameters.AddWithValue("senderbirthdateP", sBdate);
                        command.Parameters.AddWithValue("senderbranchidP", SenderBranchID);
                        command.Parameters.AddWithValue("receiverfnameP", ReceiverFName);
                        command.Parameters.AddWithValue("receiverlnameP", ReceiverLName);
                        command.Parameters.AddWithValue("receivermnameP", ReceiverMName);
                        command.Parameters.AddWithValue("receivernameP", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("receiverstreetP", ReceiverStreet);
                        command.Parameters.AddWithValue("receiverprovincecityP", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("receivercountryP", ReceiverCountry);
                        command.Parameters.AddWithValue("receivergenderP", ReceiverGender);
                        command.Parameters.AddWithValue("receivercontactnoP", ReceiverContactNo);
                        command.Parameters.AddWithValue("receiverbirthdateP", rBdate);
                        command.Parameters.AddWithValue("balanceP", balance);
                        command.Parameters.AddWithValue("dormantchargeP", DormantCharge);
                        command.Parameters.AddWithValue("servicechargeP", ServiceCharge);
                        command.Parameters.AddWithValue("vatP", vat);
                        command.Parameters.AddWithValue("remotezonecodeP", remotezone);
                        command.Parameters.AddWithValue("tableoriginatedP", tblorig);
                        command.Parameters.AddWithValue("yearP", dt.ToString("yyyy"));
                        command.Parameters.AddWithValue("pocurrency", preferredcurrency);
                        command.Parameters.AddWithValue("poamount", amountpo);
                        command.Parameters.AddWithValue("exchangerate", exchangerate);
                        command.Parameters.AddWithValue("trxntp", transtype);
                        command.ExecuteNonQuery();

                        String custS = getcustomertable(SenderLName);
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogsglobal.save_customers";
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("tblcustomer", custS);
                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("controlno", ControlNo);
                        command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("fname", SenderFName);
                        command.Parameters.AddWithValue("lname", SenderLName);
                        command.Parameters.AddWithValue("mname", SenderMName);
                        command.Parameters.AddWithValue("sobranch", SOBranch);
                        command.Parameters.AddWithValue("pobranch", POBranchName);
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("remotebranch", RemoteBranch);
                        command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                        command.Parameters.AddWithValue("status", 1);
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreator", syscreator);
                        command.Parameters.AddWithValue("customertype", "S");
                        command.Parameters.AddWithValue("amount", Principal);
                        command.ExecuteNonQuery();

                        String custR = getcustomertable(ReceiverLName);
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogsglobal.save_customers";
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("tblcustomer", custR);
                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("controlno", ControlNo);
                        command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("fname", ReceiverFName);
                        command.Parameters.AddWithValue("lname", ReceiverLName);
                        command.Parameters.AddWithValue("mname", ReceiverMName);
                        command.Parameters.AddWithValue("sobranch", SOBranch);
                        command.Parameters.AddWithValue("pobranch", POBranchName);
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("remotebranch", RemoteBranch);
                        command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                        command.Parameters.AddWithValue("status", 1);
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreator", syscreator);
                        command.Parameters.AddWithValue("customertype", "R");
                        command.Parameters.AddWithValue("amount", Principal);
                        command.ExecuteNonQuery();

                        command.Transaction = trans;
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogsglobal.savelog53";
                        command.CommandType = CommandType.StoredProcedure;

                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("action", "PAYOUT");
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("txndate", dt);
                        command.Parameters.AddWithValue("stationcode", stationcode);
                        command.Parameters.AddWithValue("stationno", Station);
                        command.Parameters.AddWithValue("zonecode", ZoneCode);
                        command.Parameters.AddWithValue("branchcode", BranchCode);
                        command.Parameters.AddWithValue("operatorid", OperatorID);
                        command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                        command.Parameters.AddWithValue("remotereason", Reason);
                        command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                        command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                        command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                        command.Parameters.AddWithValue("remotezonecode", remotezone);
                        command.Parameters.AddWithValue("type", "N");
                        command.ExecuteNonQuery();

                        kplog.Info("SUCCESS:: message : INSERT INTO Mobextransactionsglobal.payout" + month + ":: controlnoP: " + ControlNo + " kptnnoP: " + KPTNNo + " claimeddateP: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " operatoridP: " + OperatorID + " stationidP: " + Station + " isremoteP: " + IsRemote + " remotebranchP: " + RemoteBranch + " remoteoperatoridP: " + RemoteOperatorID + " reasonP: " + Reason + " sobranchP: " + SOBranch + " socontrolnoP: " + SOControlNo + " sooperatorP: " + SOOperator + " sodateP: " + SODate + " soornoP: " + ORNo + " syscreatorP: " + syscreator + " currencyP: " + Currency + " principalP: " + Principal + " relationP: " + Relation + " idtypeP: " + IDType + " idnoP: " + IDNo + " expirydateP: " + xPiry + " branchcodeP: " + BranchCode + " zonecodeP: " + ZoneCode + " sendermlcardnoP: " + SenderMLCardNO + " senderfnameP: " + SenderFName + " senderlnameP: " + SenderLName + " sendermnameP: " + SenderMName + " sendernameP: " + SenderLName + ", " + SenderFName + " " + SenderMName + " senderstreetP: " + SenderStreet + " senderprovincecityP: " + SenderProvinceCity + " sendercountryP: " + SenderCountry + " sendergenderP: " + SenderGender + " sendercontactnoP: " + SenderContactNo + " senderissmsP: " + SenderIsSMS + " senderbirthdateP: " + sBdate + " senderbranchidP: " + SenderBranchID + " receiverfnameP: " + ReceiverFName + " receiverlnameP: " + ReceiverLName + " receivermnameP: " + ReceiverMName + " receivernameP: " + ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName + " receiverstreetP: " + ReceiverStreet + " receiverprovincecityP: " + ReceiverProvinceCity + " receivercountryP: " + ReceiverCountry + " receivergenderP: " + ReceiverGender + " receivercontactnoP: " + ReceiverContactNo + " receiverbirthdateP: " + rBdate + " balanceP: " + balance + " dormantchargeP: " + DormantCharge + " servicechargeP: " + ServiceCharge + " vatP: " + vat + " remotezonecodeP: " + remotezone + " tableoriginatedP: " + tblorig + " yearP: " + dt.ToString("yyyy") + " pocurrency: " + preferredcurrency + " poamount: " + amountpo + " exchangerate: " + exchangerate + " trxntp: " + transtype);
                        kplog.Info("SUCCESS:: message : INSERT SENDER INFO LOGS:: tblcustomer: " + custS + " kptnno: " + KPTNNo + " controlno: " + ControlNo + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " fname: " + SenderFName + " lname: " + SenderLName + " mname: " + SenderMName + " sobranch: " + SOBranch + " pobranch: " + POBranchName + " isremote: " + IsRemote + " remotebranch: " + RemoteBranch + " CancelledBranch: status: 1 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " customertype: S amount: " + Principal);
                        kplog.Info("SUCCESS:: message : INSERT RECEIVER INFO LOGS:: tblcustomer: " + custR + " kptnno: " + KPTNNo + " controlno: " + ControlNo + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " fname: " + ReceiverFName + " lname: " + ReceiverLName + " mname: " + ReceiverMName + " sobranch: " + SOBranch + " pobranch: " + POBranchName + " isremote: " + IsRemote + " remotebranch: " + RemoteBranch + " CancelledBranch: status: 1 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " customertype: R amount: " + Principal);
                        kplog.Info("SUCCESS:: message : INSERT TRANSACTION LOGS:: kptnno: " + KPTNNo + " action: PAYOUT isremote: " + IsRemote + " txndate: " + dt + " stationcode: " + stationcode + " stationno: " + Station + " zonecode: " + ZoneCode + " branchcode: " + BranchCode + " operatorid: " + OperatorID + " cancelledreason: " + DBNull.Value + " remotereason: " + Reason + " remotebranch: " + (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch) + " remoteoperator: " + (RemoteOperatorID.Equals(DBNull.Value) ? null : RemoteOperatorID) + " oldkptnno: " + DBNull.Value + " remotezonecode: " + remotezone + " type: N");

                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCES:: message: " + getRespMessage(1) + ", DateClaimed: " + dt.ToString());
                        return new PayoutResponse { respcode = 1, message = getRespMessage(1), DateClaimed = dt };
                    }
                }
                catch (MySqlException ex)
                {
                   
                    Int32 respcode = 0;
                    if (ex.Number == 1062)
                    {
                        respcode = 3;
                        kplog.Error("FAILED:: message : " + getRespMessage(3));
                    }
                    trans.Rollback();
                    dbconGlobal.CloseConnection();
                    kplog.Fatal("FAILED:: message : " + getRespMessage(respcode) + " , ErrorDetail: " + ex.ToString());
                    return new PayoutResponse { respcode = respcode, message = getRespMessage(respcode) + " " + ex.Message, ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString() + ", DateClaimed: " + DateTime.Now);
            dbconGlobal.CloseConnection();
            return new PayoutResponse { respcode = 0, message = getRespMessage(0) + " " + ex.Message, ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
        }


    }

    [WebMethod(BufferResponse = false)]
    public PayoutResponse payoutGlobal(String Username, String Password, String ControlNo, String KPTNNo, String OperatorID, String Station, int IsRemote, String RemoteBranch, String RemoteOperatorID, String Reason, String SOBranch, String SOControlNo, String SOOperator, String Currency, Double Principal, String SenderID, String ReceiverID, String Relation, String IDType, String IDNo, String ExpiryDate, String SODate, int sysmodifier, String BranchCode, String series, String ZoneCode, int type, Double balance, Double DormantCharge, String senderid, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, String ORNo, Double ServiceCharge, Double version, String stationcode, int remotezone, String RemoteBranchCode, String POBranchName, double vat, string syscreator, string preferredcurrency, double amountpo, double exchangerate, String transtype)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", ControlNo: " + ControlNo + ",KPTNNo : " + KPTNNo + ", OperatorID: " + OperatorID + ",Station : " + Station + ", IsRemote: " + IsRemote + ", RemoteBranch: " + RemoteBranch + ",RemoteOperatorID : " + RemoteOperatorID + ", Reason: " + Reason + " ,SOBranch :" + SOBranch + ", SOControlNo: " + SOControlNo + ", SOOperator: " + SOOperator + ", Currency: " + Currency + ", Principal: " + Principal + ", SenderID: " + SenderID + ", RecieverID: " + ReceiverID + ", Relation: " + Relation + ", IDType: " + IDType + ", IDNo: " + IDNo + ", ExpiryDate: " + ExpiryDate + ", SODate: " + SODate + ", sysmodifier: " + sysmodifier + ", BranchCode: " + BranchCode + ", series: " + series + ", ZoneCode: " + ZoneCode + ", type: " + type + ", balance: " + balance + ", DormantCharge: " + DormantCharge + ", senderid: " + senderid + ", SenderMLCardNo: " + SenderMLCardNO + ", SenderFName: " + SenderFName + ", SenderLName: " + SenderLName + ", SenderMName: " + SenderMName + ", SenderStreet: " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ", SenderGender: " + SenderGender + ", SenderContactNo: " + SenderContactNo + ", SenderIsSMS: " + SenderIsSMS + ", SenderBirthdate: " + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", RecieverMLCardNO: " + ReceiverMLCardNO + ", RecieverFName: " + ReceiverFName + ", RecieverLName: " + ReceiverLName + ", RecieverMName: " + ReceiverMName + ", RecieverStreet: " + ReceiverStreet + ", RecieverProvinceCity: " + ReceiverProvinceCity + ", RecieverCountry: " + ReceiverCountry + ", RecieverGender: " + ReceiverGender + ", RecieverContactNo: " + ReceiverContactNo + ", RecieverBirthdate: " + ReceiverBirthdate + ", ORNo: " + ORNo + ", ServiceCharge: " + ServiceCharge + ", version: " + version + ", stationcode: " + stationcode + ", remotezone: " + remotezone + ", RemoteBranchCode: " + RemoteBranchCode + ", POBranchName: " + POBranchName + ", vat: " + vat + ", syscreator: " + syscreator + ", preferredcurrency: " + preferredcurrency + ", amountpo: " + amountpo + ", exchangerate: " + exchangerate + ", transtype: " + transtype);

        try
        {
            if (Station.ToString().Equals("0"))
            {
                kplog.Fatal("FAILED:: message  : " + getRespMessage(13));
                return new PayoutResponse { respcode = 10, message = getRespMessage(13) };
            }
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Unknown Web Service User!");
                return new PayoutResponse { respcode = 7, message = getRespMessage(7) };
            }

            int sr = ConvertSeries(series);
            dt = getServerDateGlobal(false);

            using (MySqlConnection checkinglang = dbconGlobal.getConnection())
            {
                checkinglang.Open();

                try
                {
                    using (command = checkinglang.CreateCommand())
                    {
                        string checkifcontrolexist = "select controlno from " + generateTableNameGlobal(1, null) + " where controlno=@controlno";
                        command.CommandTimeout = 0;
                        command.CommandText = checkifcontrolexist;
                        command.Parameters.AddWithValue("controlno", ControlNo);
                        MySqlDataReader controlexistreader = command.ExecuteReader();
                        if (controlexistreader.HasRows)
                        {
                            controlexistreader.Close();
                            string query101 = string.Empty;
                            if (IsRemote == 1)
                                query101 = "select max(substring(controlno,length(controlno)-5,length(controlno))) as max1 from " + generateTableNameGlobal(1, null) + " where remotebranch = @branchcode and stationid = @stationid and remotezonecode =@zonecode";
                            else
                                query101 = "select max(substring(controlno,length(controlno)-5,length(controlno))) as max1 from " + generateTableNameGlobal(1, null) + " where branchcode = @branchcode and stationid = @stationid and zonecode=@zonecode";
                            command.CommandText = query101;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("branchcode", IsRemote == 1 ? RemoteBranch : BranchCode);
                            command.Parameters.AddWithValue("stationid", Station);
                            command.Parameters.AddWithValue("zonecode", remotezone == 0 ? Convert.ToInt32(ZoneCode) : remotezone);

                            MySqlDataReader controlmaxreader = command.ExecuteReader();
                            if (controlmaxreader.Read())
                            {
                                sr = Convert.ToInt32(controlmaxreader["max"].ToString()) + 1;

                            }
                            controlmaxreader.Close();

                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("st", IsRemote == 1 ? "00" : Station);
                            command.Parameters.AddWithValue("bcode", IsRemote == 1 ? RemoteBranch : BranchCode);
                            command.Parameters.AddWithValue("series", sr);
                            command.Parameters.AddWithValue("zcode", remotezone == 0 ? Convert.ToInt32(ZoneCode) : remotezone);
                            command.Parameters.AddWithValue("tp", type);
                            int abc101 = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpformsglobal.control nseries: " + sr + " ,bcode: " + (IsRemote == 1 ? RemoteBranch : BranchCode) + " , station: " + (IsRemote == 1 ? "00" : Station) + " , zcode: " + (remotezone == 0 ? Convert.ToInt32(ZoneCode) : remotezone) + ", type: " + type);

                            checkinglang.Close();
                            dbconGlobal.CloseConnection();
                            kplog.Error("FAILED:: message : Error in saving transaction:: respcode: 101 message: Problem saving transaction. Please close the sendout payout window and try again.");
                            return new PayoutResponse { respcode = 101, message = "Problem saving transaction. Please close the sendout payout window and try again." };

                        }
                    }
                }
                catch (Exception ex)
                {
                    checkinglang.Close();
                    dbconGlobal.CloseConnection();
                    kplog.Error("FAILED:: message: Error in saving transaction:: respcode: 101 message: Problem saving transaction. Please close the sendout payout window and try again.");
                    return new PayoutResponse { respcode = 101, message = "Problem saving transaction. Please close the sendout payout window and try again." };
                }
                checkinglang.Close();
                dbconGlobal.CloseConnection();
            }

            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                try
                {

                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit = 0;";
                        command.ExecuteNonQuery();
                        //save_payout
                        command.Transaction = trans;
                        String sBdate = (SenderBirthdate == String.Empty) ? null : Convert.ToDateTime(SenderBirthdate).ToString("yyyy-MM-dd");
                        String rBdate = (ReceiverBirthdate == String.Empty) ? null : Convert.ToDateTime(ReceiverBirthdate).ToString("yyyy-MM-dd");
                        String xPiry = (ExpiryDate == String.Empty) ? null : Convert.ToDateTime(ExpiryDate).ToString("yyyy-MM-dd");
                        String month = dt.ToString("MM");
                        String tblorig = "payout" + dt.ToString("MM") + dt.ToString("dd");
                        String insert = "Insert into " + generateTableNameGlobal(1, null) + " (ControlNo, KPTNNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, SOBranch, SOControlNo, SOOperator,  Currency, Principal, Relation, IDType, IDNo, ExpiryDate, ClaimedDate, SODate, syscreated, BranchCode, ZoneCode, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS, SenderBirthdate, SenderBranchID, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, SOORNo, ServiceCharge, RemoteZoneCode,syscreator,preferredpo,amountpo,exchangerate,TransType) values (@ControlNo, @KPTNNo, @OperatorID, @StationID, @IsRemote, @RemoteBranch, @RemoteOperatorID, @Reason, @SOBranch, @SOControlNo, @SOOperator, @Currency, @Principal, @Relation, @IDType, @IDNo, @ExpiryDate, @ClaimedDate, @SODate, @syscreated, @BranchCode, @ZoneCode, @SenderMLCardNO, @SenderFName, @SenderLName, @SenderMName, @SenderName, @SenderStreet, @SenderProvinceCity, @SenderCountry, @SenderGender, @SenderContactNo, @SenderIsSMS, @SenderBirthdate, @SenderBranchID, @ReceiverFName, @ReceiverLName, @ReceiverMName, @ReceiverName, @ReceiverStreet, @ReceiverProvinceCity, @ReceiverCountry, @ReceiverGender, @ReceiverContactNo, @ReceiverBirthdate, @SOORNo, @ServiceCharge, @remotezone, @syscreator,@preferredpo,@amountpo,@exchangerate1,@transtp)";
                        command.CommandText = insert;

                        command.Parameters.AddWithValue("ControlNo", ControlNo);
                        command.Parameters.AddWithValue("KPTNNo", KPTNNo);
                        command.Parameters.AddWithValue("OperatorID", OperatorID);
                        command.Parameters.AddWithValue("StationID", Station);
                        command.Parameters.AddWithValue("IsRemote", IsRemote);
                        command.Parameters.AddWithValue("RemoteBranch", RemoteBranch);
                        command.Parameters.AddWithValue("RemoteOperatorID", RemoteOperatorID);
                        command.Parameters.AddWithValue("Reason", Reason);
                        command.Parameters.AddWithValue("SOBranch", SOBranch);
                        command.Parameters.AddWithValue("SOControlNo", SOControlNo);
                        command.Parameters.AddWithValue("SOOperator", SOOperator);
                        command.Parameters.AddWithValue("Currency", Currency);
                        command.Parameters.AddWithValue("Principal", Principal);
                        command.Parameters.AddWithValue("Relation", Relation);
                        command.Parameters.AddWithValue("IDType", IDType);
                        command.Parameters.AddWithValue("IDNo", IDNo);
                        command.Parameters.AddWithValue("ExpiryDate", xPiry);
                        command.Parameters.AddWithValue("ClaimedDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("SODate", Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreator", syscreator);
                        command.Parameters.AddWithValue("BranchCode", BranchCode);
                        command.Parameters.AddWithValue("ZoneCode", ZoneCode);
                        command.Parameters.AddWithValue("Balance", balance);
                        command.Parameters.AddWithValue("DormantCharge", DormantCharge);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", sBdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", rBdate);
                        command.Parameters.AddWithValue("SOORNo", ORNo);
                        command.Parameters.AddWithValue("ServiceCharge", ServiceCharge);
                        command.Parameters.AddWithValue("remotezone", remotezone);
                        command.Parameters.AddWithValue("vat", vat);
                        command.Parameters.AddWithValue("preferredpo", preferredcurrency);
                        command.Parameters.AddWithValue("amountpo", amountpo);
                        command.Parameters.AddWithValue("exchangerate1", exchangerate);
                        command.Parameters.AddWithValue("transtp", transtype);
                        int x = command.ExecuteNonQuery();

                        kplog.Info("SUCCESS:: message : INSERT INTO " + generateTableNameGlobal(1, null) + ":: ControlNo: " + ControlNo + " KPTNNo: " + KPTNNo + " OperatorID: " + OperatorID + " StationID: " + Station + " IsRemote: " + IsRemote + " RemoteBranch: " + RemoteBranch + " RemoteOperatorID: " + RemoteOperatorID + " Reason: " + Reason + " SOBranch: " + SOBranch + " SOControlNo: " + SOControlNo + " SOOperator: " + SOOperator + " Currency: " + Currency + " Principal: " + Principal + " Relation: " + Relation + " IDType: " + IDType + " IDNo: " + IDNo + " ExpiryDate: " + xPiry + " ClaimedDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " SODate: " + Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss") + " syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " BranchCode: " + BranchCode + " ZoneCode: " + ZoneCode + " Balance: " + balance + " DormantCharge: " + DormantCharge + " SenderMLCardNO: " + SenderMLCardNO + " SenderFName: " + SenderFName + " SenderLName: " + SenderLName + " SenderMName: " + SenderMName + " SenderName: " + SenderLName + ", " + SenderFName + " " + SenderMName + " SenderStreet: " + SenderStreet + " SenderProvinceCity: " + SenderProvinceCity + " SenderCountry: " + SenderCountry + " SenderGender: " + SenderGender + " SenderContactNo: " + SenderContactNo + " SenderIsSMS: " + SenderIsSMS + " SenderBirthDate: " + SenderBirthdate + " SenderBranchID: " + SenderBranchID + " ReceiverFName: " + ReceiverFName + " ReceiverLName: " + ReceiverMName + " ReceiverName: " + ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName + " ReceiverStreet: " + ReceiverStreet + " ReceiverProvinceCity: " + ReceiverProvinceCity + " ReceiverCountry: " + ReceiverCountry + " ReceiverGender: " + ReceiverGender + " ReceiverContactNo: " + ReceiverContactNo + " ReceiverBirthDate: " + rBdate + " SOORNo: " + ORNo + " ServiceCharge: " + ServiceCharge + " remotezone: " + remotezone + " vat: " + vat + " preferredpo: " + preferredcurrency + " amountpo: " + amountpo + " exchangerate1: " + exchangerate + " transtp: " + transtype);

                        command.CommandText = "update " + decodeKPTNGlobal(0, KPTNNo) + " set IsClaimed = 1, sysmodified = @modified, sysmodifier = @modifier where KPTNNo = @kptn";
                        command.Parameters.AddWithValue("modified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("modifier", sysmodifier);
                        command.Parameters.AddWithValue("kptn", KPTNNo);
                        int y = command.ExecuteNonQuery();

                        kplog.Info("SUCCESS:: message : UPDATE " + decodeKPTNGlobal(0, KPTNNo) + ":: IsClaimed: 1 sysmodified: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " sysmodifier: " + sysmodifier + " WHERE kptn: " + KPTNNo);

                        if (IsRemote == 1)
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "01");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);//nseries
                            command.Parameters.AddWithValue("zcode", remotezone);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();

                            kplog.Info("SUCCESS:: message : UPDATE control:: nseries: " + sr + 1 + " where bcode: " + RemoteBranch + " station: 01 zcode: " + remotezone + " type: " + type);
                        }
                        else
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", Station);
                            command.Parameters.AddWithValue("bcode", BranchCode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", ZoneCode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();

                            kplog.Info("SUCCESS:: message : UPDATE control:: nseries: " + sr + 1 + " where bcode: " + BranchCode + " station: " + Station + " zcode: " + ZoneCode + " type: " + type);
                        }

                        String insertpayout = "INSERT INTO kptransactionsglobal.payout" + month + " (controlno,kptnno,claimeddate," +
                        "operatorid,stationid,isremote,remotebranch,remoteoperatorid,reason,sobranch,socontrolno,sooperator," +
                        "sodate,soorno,syscreated,syscreator,currency,principal,relation,idtype,idno,expirydate,branchcode," +
                        "zonecode,sendermlcardno,senderfname,senderlname,sendermname,sendername,senderstreet," +
                        "senderprovincecity,sendercountry,sendergender,sendercontactno,senderissms,senderbirthdate," +
                        "senderbranchid,receiverfname,receiverlname,receivermname,receivername,receiverstreet," +
                        "receiverprovincecity,receivercountry,receivergender,receivercontactno,receiverbirthdate," +
                        "balance,dormantcharge,servicecharge,vat,remotezonecode,tableoriginated,`year`,pocurrency,poamount,exchangerate,TransType) " +
                        "values (@controlnoP,@kptnnoP,@claimeddateP," +
                        "@operatoridP,@stationidP,@isremoteP,@remotebranchP,@remoteoperatoridP,@reasonP,@sobranchP,@socontrolnoP,@sooperatorP," +
                        "@sodateP,@soornoP,@syscreatedP,@syscreatorP,@currencyP,@principalP,@relationP,@idtypeP,@idnoP,@expirydateP,@branchcodeP," +
                        "@zonecodeP,@sendermlcardnoP,@senderfnameP,@senderlnameP,@sendermnameP,@sendernameP,@senderstreetP," +
                        "@senderprovincecityP,@sendercountryP,@sendergenderP,@sendercontactnoP,@senderissmsP,@senderbirthdateP," +
                        "@senderbranchidP,@receiverfnameP,@receiverlnameP,@receivermnameP,@receivernameP,@receiverstreetP," +
                        "@receiverprovincecityP,@receivercountryP,@receivergenderP,@receivercontactnoP,@receiverbirthdateP," +
                        "@balanceP,@dormantchargeP,@servicechargeP,@vatP,@remotezonecodeP,@tableoriginatedP,@yearP,@pocurrency,@poamount,@exchangerate,@trxntp)";
                        command.CommandText = insertpayout;

                        command.Parameters.AddWithValue("controlnoP", ControlNo);
                        command.Parameters.AddWithValue("kptnnoP", KPTNNo);
                        command.Parameters.AddWithValue("claimeddateP", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("operatoridP", OperatorID);
                        command.Parameters.AddWithValue("stationidP", Station);
                        command.Parameters.AddWithValue("isremoteP", IsRemote);
                        command.Parameters.AddWithValue("remotebranchP", RemoteBranch);
                        command.Parameters.AddWithValue("remoteoperatoridP", RemoteOperatorID);
                        command.Parameters.AddWithValue("reasonP", Reason);
                        command.Parameters.AddWithValue("sobranchP", SOBranch);
                        command.Parameters.AddWithValue("socontrolnoP", SOControlNo);
                        command.Parameters.AddWithValue("sooperatorP", SOOperator);
                        command.Parameters.AddWithValue("sodateP", Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("soornoP", ORNo);
                        command.Parameters.AddWithValue("syscreatedP", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreatorP", syscreator);
                        command.Parameters.AddWithValue("currencyP", Currency);
                        command.Parameters.AddWithValue("principalP", Principal);
                        command.Parameters.AddWithValue("relationP", Relation);
                        command.Parameters.AddWithValue("idtypeP", IDType);
                        command.Parameters.AddWithValue("idnoP", IDNo);
                        command.Parameters.AddWithValue("expirydateP", xPiry);
                        command.Parameters.AddWithValue("branchcodeP", BranchCode);
                        command.Parameters.AddWithValue("zonecodeP", ZoneCode);
                        command.Parameters.AddWithValue("sendermlcardnoP", SenderMLCardNO);
                        command.Parameters.AddWithValue("senderfnameP", SenderFName);
                        command.Parameters.AddWithValue("senderlnameP", SenderLName);
                        command.Parameters.AddWithValue("sendermnameP", SenderMName);
                        command.Parameters.AddWithValue("sendernameP", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("senderstreetP", SenderStreet);
                        command.Parameters.AddWithValue("senderprovincecityP", SenderProvinceCity);
                        command.Parameters.AddWithValue("sendercountryP", SenderCountry);
                        command.Parameters.AddWithValue("sendergenderP", SenderGender);
                        command.Parameters.AddWithValue("sendercontactnoP", SenderContactNo);
                        command.Parameters.AddWithValue("senderissmsP", SenderIsSMS);
                        command.Parameters.AddWithValue("senderbirthdateP", sBdate);
                        command.Parameters.AddWithValue("senderbranchidP", SenderBranchID);
                        command.Parameters.AddWithValue("receiverfnameP", ReceiverFName);
                        command.Parameters.AddWithValue("receiverlnameP", ReceiverLName);
                        command.Parameters.AddWithValue("receivermnameP", ReceiverMName);
                        command.Parameters.AddWithValue("receivernameP", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("receiverstreetP", ReceiverStreet);
                        command.Parameters.AddWithValue("receiverprovincecityP", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("receivercountryP", ReceiverCountry);
                        command.Parameters.AddWithValue("receivergenderP", ReceiverGender);
                        command.Parameters.AddWithValue("receivercontactnoP", ReceiverContactNo);
                        command.Parameters.AddWithValue("receiverbirthdateP", rBdate);
                        command.Parameters.AddWithValue("balanceP", balance);
                        command.Parameters.AddWithValue("dormantchargeP", DormantCharge);
                        command.Parameters.AddWithValue("servicechargeP", ServiceCharge);
                        command.Parameters.AddWithValue("vatP", vat);
                        command.Parameters.AddWithValue("remotezonecodeP", remotezone);
                        command.Parameters.AddWithValue("tableoriginatedP", tblorig);
                        command.Parameters.AddWithValue("yearP", dt.ToString("yyyy"));
                        command.Parameters.AddWithValue("pocurrency", preferredcurrency);
                        command.Parameters.AddWithValue("poamount", amountpo);
                        command.Parameters.AddWithValue("exchangerate", exchangerate);
                        command.Parameters.AddWithValue("trxntp", transtype);
                        command.ExecuteNonQuery();

                        String custS = getcustomertable(SenderLName);
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogsglobal.save_customers";
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("tblcustomer", custS);
                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("controlno", ControlNo);
                        command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("fname", SenderFName);
                        command.Parameters.AddWithValue("lname", SenderLName);
                        command.Parameters.AddWithValue("mname", SenderMName);
                        command.Parameters.AddWithValue("sobranch", SOBranch);
                        command.Parameters.AddWithValue("pobranch", POBranchName);
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("remotebranch", RemoteBranch);
                        command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                        command.Parameters.AddWithValue("status", 1);
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreator", syscreator);
                        command.Parameters.AddWithValue("customertype", "S");
                        command.Parameters.AddWithValue("amount", Principal);
                        command.ExecuteNonQuery();

                        String custR = getcustomertable(ReceiverLName);
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogsglobal.save_customers";
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("tblcustomer", custR);
                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("controlno", ControlNo);
                        command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("fname", ReceiverFName);
                        command.Parameters.AddWithValue("lname", ReceiverLName);
                        command.Parameters.AddWithValue("mname", ReceiverMName);
                        command.Parameters.AddWithValue("sobranch", SOBranch);
                        command.Parameters.AddWithValue("pobranch", POBranchName);
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("remotebranch", RemoteBranch);
                        command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                        command.Parameters.AddWithValue("status", 1);
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreator", syscreator);
                        command.Parameters.AddWithValue("customertype", "R");
                        command.Parameters.AddWithValue("amount", Principal);
                        command.ExecuteNonQuery();

                        command.Transaction = trans;
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogsglobal.savelog53";
                        command.CommandType = CommandType.StoredProcedure;

                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("action", "PAYOUT");
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("txndate", dt);
                        command.Parameters.AddWithValue("stationcode", stationcode);
                        command.Parameters.AddWithValue("stationno", Station);
                        command.Parameters.AddWithValue("zonecode", ZoneCode);
                        command.Parameters.AddWithValue("branchcode", BranchCode);
                        command.Parameters.AddWithValue("operatorid", OperatorID);
                        command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                        command.Parameters.AddWithValue("remotereason", Reason);
                        command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                        command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                        command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                        command.Parameters.AddWithValue("remotezonecode", remotezone);
                        command.Parameters.AddWithValue("type", "N");
                        command.ExecuteNonQuery();

                        kplog.Info("SUCCESS:: message : INSERT INTO kptransactionsglobal.payout" + month + ":: controlnoP: " + ControlNo + " kptnnoP: " + KPTNNo + " claimeddateP: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " operatoridP: " + OperatorID + " stationidP: " + Station + " isremoteP: " + IsRemote + " remotebranchP: " + RemoteBranch + " remoteoperatoridP: " + RemoteOperatorID + " reasonP: " + Reason + " sobranchP: " + SOBranch + " socontrolnoP: " + SOControlNo + " sooperatorP: " + SOOperator + " sodateP: " + SODate + " soornoP: " + ORNo + " syscreatorP: " + syscreator + " currencyP: " + Currency + " principalP: " + Principal + " relationP: " + Relation + " idtypeP: " + IDType + " idnoP: " + IDNo + " expirydateP: " + xPiry + " branchcodeP: " + BranchCode + " zonecodeP: " + ZoneCode + " sendermlcardnoP: " + SenderMLCardNO + " senderfnameP: " + SenderFName + " senderlnameP: " + SenderLName + " sendermnameP: " + SenderMName + " sendernameP: " + SenderLName + ", " + SenderFName + " " + SenderMName + " senderstreetP: " + SenderStreet + " senderprovincecityP: " + SenderProvinceCity + " sendercountryP: " + SenderCountry + " sendergenderP: " + SenderGender + " sendercontactnoP: " + SenderContactNo + " senderissmsP: " + SenderIsSMS + " senderbirthdateP: " + sBdate + " senderbranchidP: " + SenderBranchID + " receiverfnameP: " + ReceiverFName + " receiverlnameP: " + ReceiverLName + " receivermnameP: " + ReceiverMName + " receivernameP: " + ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName + " receiverstreetP: " + ReceiverStreet + " receiverprovincecityP: " + ReceiverProvinceCity + " receivercountryP: " + ReceiverCountry + " receivergenderP: " + ReceiverGender + " receivercontactnoP: " + ReceiverContactNo + " receiverbirthdateP: " + rBdate + " balanceP: " + balance + " dormantchargeP: " + DormantCharge + " servicechargeP: " + ServiceCharge + " vatP: " + vat + " remotezonecodeP: " + remotezone + " tableoriginatedP: " + tblorig + " yearP: " + dt.ToString("yyyy") + " pocurrency: " + preferredcurrency + " poamount: " + amountpo + " exchangerate: " + exchangerate + " trxntp: " + transtype);
                        kplog.Info("SUCCESS:: message : INSERT SENDER INFO LOGS:: tblcustomer: " + custS + " kptnno: " + KPTNNo + " controlno: " + ControlNo + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " fname: " + SenderFName + " lname: " + SenderLName + " mname: " + SenderMName + " sobranch: " + SOBranch + " pobranch: " + POBranchName + " isremote: " + IsRemote + " remotebranch: " + RemoteBranch + " CancelledBranch: status: 1 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " customertype: S amount: " + Principal);
                        kplog.Info("SUCCESS:: message : INSERT RECEIVER INFO LOGS:: tblcustomer: " + custR + " kptnno: " + KPTNNo + " controlno: " + ControlNo + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " fname: " + ReceiverFName + " lname: " + ReceiverLName + " mname: " + ReceiverMName + " sobranch: " + SOBranch + " pobranch: " + POBranchName + " isremote: " + IsRemote + " remotebranch: " + RemoteBranch + " CancelledBranch: status: 1 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " customertype: R amount: " + Principal);
                        kplog.Info("SUCCESS:: message : INSERT TRANSACTION LOGS:: kptnno: " + KPTNNo + " action: PAYOUT isremote: " + IsRemote + " txndate: " + dt + " stationcode: " + stationcode + " stationno: " + Station + " zonecode: " + ZoneCode + " branchcode: " + BranchCode + " operatorid: " + OperatorID + " cancelledreason: " + DBNull.Value + " remotereason: " + Reason + " remotebranch: " + (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch) + " remoteoperator: " + (RemoteOperatorID.Equals(DBNull.Value) ? null : RemoteOperatorID) + " oldkptnno: " + DBNull.Value + " remotezonecode: " + remotezone + " type: N");

                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: message: " + getRespMessage(1) + ", DateClaimed: " + dt);
                        return new PayoutResponse { respcode = 1, message = getRespMessage(1), DateClaimed = dt };
                    }
                }
                catch (MySqlException ex)
                {
                    kplog.Error(ex.ToString());
                    Int32 respcode = 0;
                    if (ex.Number == 1062)
                    {
                        respcode = 3;
                        kplog.Error("FAILED:: message : " + getRespMessage(3));
                    }

                    trans.Rollback();
                    dbconGlobal.CloseConnection();
                    kplog.Fatal("FAILED:: respcode: " + respcode + " , message : " + getRespMessage(respcode) + " " + ex.Message + ", ErrorDetail: " + ex.ToString() + ", DateClaimed: " + DateTime.Now);
                    return new PayoutResponse { respcode = respcode, message = getRespMessage(respcode) + " " + ex.Message, ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
                }
            }
        }
        catch (Exception ex)
        {
    
            dbconGlobal.CloseConnection();
            kplog.Fatal("FAILED:: respcode : 0 , message : " + getRespMessage(0) + " " + ex.Message + ", ErrorDetail: " + ex.ToString() + " , DateClaimed: " + DateTime.Now);
            return new PayoutResponse { respcode = 0, message = getRespMessage(0) + " " + ex.Message, ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
        }


    }


    [WebMethod(BufferResponse = false)]
    public PayoutResponse payoutDomestic(String Username, String Password, String ControlNo, String KPTNNo, String OperatorID, String Station, int IsRemote, String RemoteBranch, String RemoteOperatorID, String Reason, String SOBranch, String SOControlNo, String SOOperator, String Currency, Decimal Principal, String SenderID, String ReceiverID, String Relation, String IDType, String IDNo, String ExpiryDate, String SODate, int sysmodifier, String BranchCode, String series, String ZoneCode, Int32 type, Decimal balance, Decimal DormantCharge, String senderid, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, String ORNo, Double ServiceCharge, Double version, String stationcode, Int32 remotezone, String RemoteBranchCode, String POBranchName)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", ControlNo: " + ControlNo + ",KPTNNo : " + KPTNNo + ", OperatorID: " + OperatorID + ",Station : " + Station + ", IsRemote: " + IsRemote + ", RemoteBranch: " + RemoteBranch + ",RemoteOperatorID : " + RemoteOperatorID + ", Reason: " + Reason + " ,SOBranch :" + SOBranch + ", SOControlNo: " + SOControlNo + ", SOOperator: " + SOOperator + ", Currency: " + Currency + ", Principal: " + Principal + ", SenderID: " + SenderID + ", RecieverID: " + ReceiverID + ", Relation: " + Relation + ", IDType: " + IDType + ", IDNo: " + IDNo + ", ExpiryDate: " + ExpiryDate + ", SODate: " + SODate + ", sysmodifier: " + sysmodifier + ", BranchCode: " + BranchCode + ", series: " + series + ", ZoneCode: " + ZoneCode + ", type: " + type + ", balance: " + balance + ", DormantCharge: " + DormantCharge + ", senderid: " + senderid + ", SenderMLCardNo: " + SenderMLCardNO + ", SenderFName: " + SenderFName + ", SenderLName: " + SenderLName + ", SenderMName: " + SenderMName + ", SenderStreet: " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ", SenderGender: " + SenderGender + ", SenderContactNo: " + SenderContactNo + ", SenderIsSMS: " + SenderIsSMS + ", SenderBirthdate: " + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", RecieverMLCardNO: " + ReceiverMLCardNO + ", RecieverFName: " + ReceiverFName + ", RecieverLName: " + ReceiverLName + ", RecieverMName: " + ReceiverMName + ", RecieverStreet: " + ReceiverStreet + ", RecieverProvinceCity: " + ReceiverProvinceCity + ", RecieverCountry: " + ReceiverCountry + ", RecieverGender: " + ReceiverGender + ", RecieverContactNo: " + ReceiverContactNo + ", RecieverBirthdate: " + ReceiverBirthdate + ", ORNo: " + ORNo + ", ServiceCharge: " + ServiceCharge + ", version: " + version + ", stationcode: " + stationcode + ", remotezone: " + remotezone + ", RemoteBranchCode: " + RemoteBranchCode + ", POBranchName: " + POBranchName);

        try
        {
            if (Station.ToString().Equals("0"))
            {
                kplog.Fatal("FAILED:: message: " + getRespMessage(13));
                return new PayoutResponse { respcode = 10, message = getRespMessage(13) };
            }
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new PayoutResponse { respcode = 7, message = getRespMessage(7) };
            }
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new PayoutResponse { respcode = 10, message = getRespMessage(10) };
            //}

            using (MySqlConnection conn = dbconDomestic.getConnection())
            {
                try
                {

                    conn.Open();
                    int sr = ConvertSeries(series);
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit = 0;";
                        command.ExecuteNonQuery();

                        command.Transaction = trans;
                        dt = getServerDateDomestic(true);
                        String sBdate = (SenderBirthdate == String.Empty) ? null : Convert.ToDateTime(SenderBirthdate).ToString("yyyy-MM-dd");
                        String rBdate = (ReceiverBirthdate == String.Empty) ? null : Convert.ToDateTime(ReceiverBirthdate).ToString("yyyy-MM-dd");
                        String xPiry = (ExpiryDate == String.Empty) ? null : Convert.ToDateTime(ExpiryDate).ToString("yyyy-MM-dd");
                        //String insert = "Insert into "+ generateTableName(1) +" (ControlNo, KPTNNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, SOBranch, SOControlNo, SOOperator,  Currency, Principal, SenderID, ReceiverID, Relation, IDType, IDNo, ExpiryDate, ClaimedDate, SODate, syscreated, BranchCode, ZoneCode, Balance, DormantCharge) values (@ControlNo, @KPTNNo, @OperatorID, @StationID, @IsRemote, @RemoteBranch, @RemoteOperatorID, @Reason, @SOBranch, @SOControlNo, @SOOperator, @Currency, @Principal, @SenderID, @ReceiverID, @Relation, @IDType, @IDNo, @ExpiryDate, @ClaimedDate, @SODate, @syscreated, @BranchCode, @ZoneCode, @Balance, @DormantCharge)";
                        String insert = "Insert into " + generateTableNameDomestic(1, null) + " (ControlNo, KPTNNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, SOBranch, SOControlNo, SOOperator,  Currency, Principal, SenderID, ReceiverID, Relation, IDType, IDNo, ExpiryDate, ClaimedDate, SODate, syscreated, BranchCode, ZoneCode, CustID, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS, SenderBirthdate, SenderBranchID, ReceiverMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, SOORNo, ServiceCharge, RemoteZoneCode) values (@ControlNo, @KPTNNo, @OperatorID, @StationID, @IsRemote, @RemoteBranch, @RemoteOperatorID, @Reason, @SOBranch, @SOControlNo, @SOOperator, @Currency, @Principal, @SenderID, @ReceiverID, @Relation, @IDType, @IDNo, @ExpiryDate, @ClaimedDate, @SODate, @syscreated, @BranchCode, @ZoneCode, @CustID, @SenderMLCardNO, @SenderFName, @SenderLName, @SenderMName, @SenderName, @SenderStreet, @SenderProvinceCity, @SenderCountry, @SenderGender, @SenderContactNo, @SenderIsSMS, @SenderBirthdate, @SenderBranchID, @ReceiverMLCardNo, @ReceiverFName, @ReceiverLName, @ReceiverMName, @ReceiverName, @ReceiverStreet, @ReceiverProvinceCity, @ReceiverCountry, @ReceiverGender, @ReceiverContactNo, @ReceiverBirthdate, @SOORNo, @ServiceCharge, @remotezone)";
                        command.CommandText = insert;

                        command.Parameters.AddWithValue("ControlNo", ControlNo);
                        command.Parameters.AddWithValue("KPTNNo", KPTNNo);
                        command.Parameters.AddWithValue("OperatorID", OperatorID);
                        command.Parameters.AddWithValue("StationID", Station);
                        command.Parameters.AddWithValue("IsRemote", IsRemote);
                        command.Parameters.AddWithValue("RemoteBranch", RemoteBranch);
                        command.Parameters.AddWithValue("RemoteOperatorID", RemoteOperatorID);
                        command.Parameters.AddWithValue("Reason", Reason);
                        command.Parameters.AddWithValue("SOBranch", SOBranch);
                        command.Parameters.AddWithValue("SOControlNo", SOControlNo);
                        command.Parameters.AddWithValue("SOOperator", SOOperator);
                        command.Parameters.AddWithValue("Currency", Currency);
                        command.Parameters.AddWithValue("Principal", Principal);
                        command.Parameters.AddWithValue("SenderID", SenderID);
                        command.Parameters.AddWithValue("ReceiverID", ReceiverID);
                        command.Parameters.AddWithValue("Relation", Relation);
                        command.Parameters.AddWithValue("IDType", IDType);
                        command.Parameters.AddWithValue("IDNo", IDNo);
                        command.Parameters.AddWithValue("ExpiryDate", (xPiry == String.Empty) ? null : xPiry);
                        command.Parameters.AddWithValue("ClaimedDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("SODate", Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("BranchCode", BranchCode);
                        command.Parameters.AddWithValue("ZoneCode", ZoneCode);
                        command.Parameters.AddWithValue("Balance", balance);
                        command.Parameters.AddWithValue("DormantCharge", DormantCharge);
                        command.Parameters.AddWithValue("CustID", senderid);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", sBdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverMLCardNO", ReceiverMLCardNO);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", rBdate);
                        //command.Parameters.AddWithValue("kptn4", kptn4);
                        command.Parameters.AddWithValue("SOORNo", ORNo);
                        command.Parameters.AddWithValue("ServiceCharge", ServiceCharge);
                        command.Parameters.AddWithValue("remotezone", remotezone);
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message : Insert into " + generateTableNameDomestic(1, null) + " : "
                       + " ControlNo: " + ControlNo
                       + " KPTNNo: " + KPTNNo
                       + " OperatorID: " + OperatorID
                       + " StationID: " + Station
                       + " IsRemote: " + IsRemote
                       + " RemoteBranch: " + RemoteBranch
                       + " RemoteOperatorID: " + RemoteOperatorID
                       + " Reason: " + Reason
                       + " SOBranch: " + SOBranch
                       + " SOControlNo: " + SOControlNo
                       + " SOOperator: " + SOOperator
                       + " Currency: " + Currency
                       + " Principal: " + Principal
                       + " SenderID: " + SenderID
                       + " ReceiverID: " + ReceiverID
                       + " Relation: " + Relation
                       + " IDType: " + IDType
                       + " IDNo: " + IDNo
                       + " ExpiryDate: " + (xPiry == String.Empty ? null : xPiry)
                       + " ClaimedDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                       + " SODate: " + SODate
                       + " syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                       + " BranchCode: " + BranchCode
                       + " ZoneCode: " + ZoneCode
                       + " Balance: " + balance
                       + " DormantCharge: " + DormantCharge
                       + " CustID: " + senderid
                       + " SenderMLCardNO: " + SenderMLCardNO
                       + " SenderFName: " + SenderFName
                       + " SenderLName: " + SenderLName
                       + " SenderMName: " + SenderMName
                       + " SenderName: " + SenderLName + " , " + SenderFName + " " + SenderMName
                       + " SenderStreet: " + SenderStreet
                       + " SenderProvinceCity: " + SenderProvinceCity
                       + " SenderCountry: " + SenderCountry
                       + " SenderGender: " + SenderGender
                       + " SenderContactNo: " + SenderContactNo
                       + " SenderIsSMS: " + SenderIsSMS
                       + " SenderBirthdate: " + sBdate
                       + " SenderBranchID: " + SenderBranchID
                       + " ReceiverMLCardNO: " + ReceiverMLCardNO
                       + " ReceiverFName: " + ReceiverFName
                       + " ReceiverLName: " + ReceiverLName
                       + " ReceiverMName: " + ReceiverMName
                       + " ReceiverName: " + ReceiverLName + " , + " + ReceiverFName + " " + ReceiverMName
                       + " ReceiverStreet: " + ReceiverStreet
                       + " ReceiverProvinceCity: " + ReceiverProvinceCity
                       + " ReceiverCountry: " + ReceiverCountry
                       + " ReceiverGender: " + ReceiverGender
                       + " ReceiverContactNo: " + ReceiverContactNo
                       + " ReceiverBirthdate: " + rBdate
                       + " SOORNo: " + ORNo
                       + " ServiceCharge: " + ServiceCharge
                       + " remotezone: " + remotezone);


                        command.CommandText = "update " + decodeKPTNDomestic(0, KPTNNo) + " set IsClaimed = 1, sysmodified = @modified, sysmodifier = @modifier where KPTNNo = @kptn";
                        command.Parameters.AddWithValue("modified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("modifier", sysmodifier);
                        command.Parameters.AddWithValue("kptn", KPTNNo);
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message : update " + decodeKPTNDomestic(0, KPTNNo) + " : "
                         + " modified: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                         + " modifier: " + sysmodifier
                         + " kptn: " + KPTNNo);


                        //command.CommandText = "update " + generateTableName(2, null) + " set IsClaimed = 1, DateClaimed = @dtClaimed where KPTN6 = @kptn1 OR MLKP4TN = @kptn1";
                        //command.Parameters.AddWithValue("kptn1", KPTNNo);
                        //command.Parameters.AddWithValue("dtClaimed", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        //command.ExecuteNonQuery();

                        //command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and userid = @uid and zcode = @zcode and type = 1";
                        //command.Parameters.AddWithValue("bcode", BranchCode);
                        //command.Parameters.AddWithValue("uid", OperatorID);
                        //command.Parameters.AddWithValue("series", sr);
                        //command.Parameters.AddWithValue("zcode", ZoneCode);

                        //throw new Exception(IsRemote.ToString() + " " + series);
                        if (IsRemote == 1)
                        {
                            command.CommandText = "update kpforms.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "01");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", remotezone);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message : update kpforms.control: "
                            + " station: " + "01"
                            + " bcode: " + RemoteBranch
                            + " nseries: " + (sr + 1)
                            + " zcode: " + remotezone
                            + " type: " + type);
                        }
                        else
                        {
                            command.CommandText = "update kpforms.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", Station);
                            command.Parameters.AddWithValue("bcode", BranchCode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", ZoneCode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message : update kpforms.control: "
                               + " station: " + Station
                               + " bcode: " + BranchCode
                               + " nseries: " + (sr + 1)
                               + " zcode: " + ZoneCode
                               + " type: " + type);
                        }


                        command.Transaction = trans;
                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogs.savelog53";
                        command.CommandType = CommandType.StoredProcedure;

                        command.Parameters.AddWithValue("kptnno", KPTNNo);
                        command.Parameters.AddWithValue("action", "PAYOUT");
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("txndate", dt);
                        command.Parameters.AddWithValue("stationcode", stationcode);
                        command.Parameters.AddWithValue("stationno", Station);
                        command.Parameters.AddWithValue("zonecode", ZoneCode);
                        command.Parameters.AddWithValue("branchcode", BranchCode);
                        command.Parameters.AddWithValue("branchname", POBranchName);
                        command.Parameters.AddWithValue("operatorid", OperatorID);
                        command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                        command.Parameters.AddWithValue("remotereason", Reason);
                        command.Parameters.AddWithValue("remotebranch", (RemoteBranchCode.Equals(DBNull.Value)) ? null : RemoteBranchCode);
                        command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                        command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                        command.Parameters.AddWithValue("remotezonecode", remotezone);
                        command.Parameters.AddWithValue("type", "N");
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message : stored proc: kpadminlogs.savelog53: "
                       + " kptnno: " + KPTNNo
                       + " action: " + "PAYOUT"
                       + " isremote: " + IsRemote
                       + " txndate: " + dt
                       + " stationcode: " + stationcode
                       + " stationno: " + Station
                       + " zonecode: " + ZoneCode
                       + " branchcode: " + BranchCode
                       + " branchname: " + POBranchName
                       + " operatorid: " + OperatorID
                       + " cancelledreason: " + DBNull.Value
                       + " remotereason: " + Reason
                       + " remotebranch: " + DBNull.Value
                       + " remoteoperator: " + DBNull.Value
                       + " oldkptnno: " + DBNull.Value
                       + " remotezonecode: " + remotezone
                       + " type: " + "N");

                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: message: " + getRespMessage(1) + ", DateClaimed: " + dt);
                        return new PayoutResponse { respcode = 1, message = getRespMessage(1), DateClaimed = dt };
                    }
                }
                catch (MySqlException ex)
                {
                   
                    Int32 respcode = 0;
                    //String message;
                    //throw new Exception(ex.ErrorCode.ToString());
                    if (ex.Number == 1062)
                    {
                        respcode = 3;
                        kplog.Error("FAILED:: message: " + getRespMessage(3));
                    }
                    //if (ex.Message.Contains("Duplicate"))
                    //{
                    //    respcode = 3;
                    //}
                    trans.Rollback();
                    dbconDomestic.CloseConnection();
                    kplog.Fatal("FAILED:: respcode: " + respcode + ", message : " + getRespMessage(respcode) + " " + ex.Message + ", ErrorDetail: " + ex.ToString() + ", DateClaimed: " + DateTime.Now);
                    return new PayoutResponse { respcode = respcode, message = getRespMessage(respcode) + " " + ex.Message, ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
                }
            }
        }
        catch (Exception ex)
        {
            //trans.Rollback();
            kplog.Fatal("FAILED:: respcode:  0 , message: " + getRespMessage(0) + " " + ex.Message + ", ErrorDetail: " + ex.ToString() + ", DateClaimed: " + DateTime.Now);
            dbconDomestic.CloseConnection();

            return new PayoutResponse { respcode = 0, message = getRespMessage(0) + " " + ex.Message, ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
        }


    }


    [WebMethod(BufferResponse = false)]
    public PayoutResponse payoutCancel(String Username, String Password, String ControlNo, String KPTNNo, String OperatorID, String Station, int IsRemote, String RemoteBranch, String RemoteOperatorID, String Reason, String SOBranch, String SOControlNo, String SOOperator, String Currency, Decimal Principal, String SenderID, String ReceiverID, String Relation, String IDType, String IDNo, String ExpiryDate, String SODate, int sysmodifier, String BranchCode, String series, String ZoneCode, Int32 type, Decimal balance, Decimal DormantCharge, String senderid, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, String ORNo, Double version, String stationcode, String irno, String CancelReason, Double CancelCharge, int CancelledByStationID, int CancelledByZoneCode, String CancelledByBranchCode, String CancelledByOperatorID, String CancelledType, String transtype)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", ControlNo: " + ControlNo + ",KPTNNo : " + KPTNNo + ", OperatorID: " + OperatorID + ",Station : " + Station + ", IsRemote: " + IsRemote + ", RemoteBranch: " + RemoteBranch + ",RemoteOperatorID : " + RemoteOperatorID + ", Reason: " + Reason + " ,SOBranch :" + SOBranch + ", SOControlNo: " + SOControlNo + ", SOOperator: " + SOOperator + ", Currency: " + Currency + ", Principal: " + Principal + ", SenderID: " + SenderID + ", RecieverID: " + ReceiverID + ", Relation: " + Relation + ", IDType: " + IDType + ", IDNo: " + IDNo + ", ExpiryDate: " + ExpiryDate + ", SODate: " + SODate + ", sysmodifier: " + sysmodifier + ", BranchCode: " + BranchCode + ", series: " + series + ", ZoneCode: " + ZoneCode + ", type: " + type + ", balance: " + balance + ", DormantCharge: " + DormantCharge + ", senderid: " + senderid + ", SenderMLCardNo: " + SenderMLCardNO + ", SenderFName: " + SenderFName + ", SenderLName: " + SenderLName + ", SenderMName: " + SenderMName + ", SenderStreet: " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ", SenderGender: " + SenderGender + ", SenderContactNo: " + SenderContactNo + ", SenderIsSMS: " + SenderIsSMS + ", SenderBirthdate: " + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", RecieverMLCardNO: " + ReceiverMLCardNO + ", RecieverFName: " + ReceiverFName + ", RecieverLName: " + ReceiverLName + ", RecieverMName: " + ReceiverMName + ", RecieverStreet: " + ReceiverStreet + ", RecieverProvinceCity: " + ReceiverProvinceCity + ", RecieverCountry: " + ReceiverCountry + ", RecieverGender: " + ReceiverGender + ", RecieverContactNo: " + ReceiverContactNo + ", RecieverBirthdate: " + ReceiverBirthdate + ", ORNo: " + ORNo  + ", version: " + version + ", stationcode: " + stationcode + ", transtype: " + transtype);

        if (Station.ToString().Equals("0"))
        {
            kplog.Fatal("FAILED:: respcode: 10, message : " + getRespMessage(13));
            return new PayoutResponse { respcode = 10, message = getRespMessage(13) };
        }
        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new PayoutResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new PayoutResponse { respcode = 10, message = getRespMessage(10) };
        //}
        try
        {
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                try
                {

                    conn.Open();
                    int sr = ConvertSeries(series);
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit=0;";
                        command.ExecuteNonQuery();

                        command.Transaction = trans;
                        dt = getServerDateGlobal(true);
                        String sBdate = (SenderBirthdate == String.Empty) ? null : Convert.ToDateTime(SenderBirthdate).ToString("yyyy-MM-dd");
                        String rBdate = (ReceiverBirthdate == String.Empty) ? null : Convert.ToDateTime(ReceiverBirthdate).ToString("yyyy-MM-dd");
                        String xPiry = (ExpiryDate == String.Empty) ? null : Convert.ToDateTime(ExpiryDate).ToString("yyyy-MM-dd");
                        //String insert = "Insert into "+ generateTableName(1) +" (ControlNo, KPTNNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, SOBranch, SOControlNo, SOOperator,  Currency, Principal, SenderID, ReceiverID, Relation, IDType, IDNo, ExpiryDate, ClaimedDate, SODate, syscreated, BranchCode, ZoneCode, Balance, DormantCharge) values (@ControlNo, @KPTNNo, @OperatorID, @StationID, @IsRemote, @RemoteBranch, @RemoteOperatorID, @Reason, @SOBranch, @SOControlNo, @SOOperator, @Currency, @Principal, @SenderID, @ReceiverID, @Relation, @IDType, @IDNo, @ExpiryDate, @ClaimedDate, @SODate, @syscreated, @BranchCode, @ZoneCode, @Balance, @DormantCharge)";
                        String insert = "Insert into " + generateTableNameGlobal(1, null) + " (ControlNo, KPTNNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, SOBranch, SOControlNo, SOOperator,  Currency, Principal, SenderID, ReceiverID, Relation, IDType, IDNo, ExpiryDate, ClaimedDate, SODate, syscreated, BranchCode, ZoneCode, CustID, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS, SenderBirthdate, SenderBranchID, ReceiverMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, SOORNo, IRNo, CancelledDate, CancelledByOperatorID, CancelledByStationID,CancelledType, CancelledReason, CancelledCustCharge, CancelledByBranchCode, TransType ) values (@ControlNo, @KPTNNo, @OperatorID, @StationID, @IsRemote, @RemoteBranch, @RemoteOperatorID, @Reason, @SOBranch, @SOControlNo, @SOOperator, @Currency, @Principal, @SenderID, @ReceiverID, @Relation, @IDType, @IDNo, @ExpiryDate, @ClaimedDate, @SODate, @syscreated, @BranchCode, @ZoneCode, @CustID, @SenderMLCardNO, @SenderFName, @SenderLName, @SenderMName, @SenderName, @SenderStreet, @SenderProvinceCity, @SenderCountry, @SenderGender, @SenderContactNo, @SenderIsSMS, @SenderBirthdate, @SenderBranchID, @ReceiverMLCardNo, @ReceiverFName, @ReceiverLName, @ReceiverMName, @ReceiverName, @ReceiverStreet, @ReceiverProvinceCity, @ReceiverCountry, @ReceiverGender, @ReceiverContactNo, @ReceiverBirthdate, @SOORNo, @IRNo, NOW(), @CancelledByOperatorID,@ CancelledByStationID, @CancelledType, @CancelledReason, @CancelledCustCharge, @CancelledByBranchCode, @transtp )";
                        command.CommandText = insert;

                        command.Parameters.AddWithValue("ControlNo", ControlNo);
                        command.Parameters.AddWithValue("KPTNNo", KPTNNo);
                        command.Parameters.AddWithValue("OperatorID", OperatorID);
                        command.Parameters.AddWithValue("StationID", Station);
                        command.Parameters.AddWithValue("IsRemote", IsRemote);
                        command.Parameters.AddWithValue("RemoteBranch", RemoteBranch);
                        command.Parameters.AddWithValue("RemoteOperatorID", RemoteOperatorID);
                        command.Parameters.AddWithValue("Reason", Reason);
                        command.Parameters.AddWithValue("SOBranch", SOBranch);
                        command.Parameters.AddWithValue("SOControlNo", SOControlNo);
                        command.Parameters.AddWithValue("SOOperator", SOOperator);
                        command.Parameters.AddWithValue("Currency", Currency);
                        command.Parameters.AddWithValue("Principal", Principal);
                        command.Parameters.AddWithValue("SenderID", SenderID);
                        command.Parameters.AddWithValue("ReceiverID", ReceiverID);
                        command.Parameters.AddWithValue("Relation", Relation);
                        command.Parameters.AddWithValue("IDType", IDType);
                        command.Parameters.AddWithValue("IDNo", IDNo);
                        command.Parameters.AddWithValue("ExpiryDate", (xPiry == String.Empty) ? null : xPiry);
                        command.Parameters.AddWithValue("ClaimedDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("SODate", Convert.ToDateTime(SODate).ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("BranchCode", BranchCode);
                        command.Parameters.AddWithValue("ZoneCode", ZoneCode);
                        command.Parameters.AddWithValue("Balance", balance);
                        command.Parameters.AddWithValue("DormantCharge", DormantCharge);
                        command.Parameters.AddWithValue("CustID", senderid);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", sBdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverMLCardNO", ReceiverMLCardNO);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", rBdate);
                        //command.Parameters.AddWithValue("kptn4", kptn4);
                        command.Parameters.AddWithValue("SOORNo", ORNo);
                        command.Parameters.AddWithValue("IRNo", irno);
                        command.Parameters.AddWithValue("CancelledByOperatorID", CancelledByOperatorID);
                        command.Parameters.AddWithValue("CancelledByStationID", CancelledByStationID);
                        command.Parameters.AddWithValue("CancelledType", CancelledType);
                        command.Parameters.AddWithValue("CancelledReason", CancelReason);
                        command.Parameters.AddWithValue("CancelledCustCharge", CancelCharge);
                        command.Parameters.AddWithValue("CancelledByBranchCode", CancelledByBranchCode);
                        command.Parameters.AddWithValue("transtp", transtype);

                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS: message : Insert into " + generateTableNameGlobal(1, null) + " :  "
                        + "  ControlNo: " + ControlNo
                        + "  KPTNNo: " + KPTNNo
                        + "  OperatorID: " + OperatorID
                        + "  StationID: " + Station
                        + "  IsRemote: " + IsRemote
                        + "  RemoteBranch: " + RemoteBranch
                        + "  RemoteOperatorID: " + RemoteOperatorID
                        + "  Reason: " + Reason
                        + "  SOBranch: " + SOBranch
                        + "  SOControlNo: " + SOControlNo
                        + "  SOOperator: " + SOOperator
                        + "  Currency: " + Currency
                        + "  Principal: " + Principal
                        + "  SenderID: " + SenderID
                        + "  ReceiverID: " + ReceiverID
                        + "  Relation: " + Relation
                        + "  IDType: " + IDType
                        + "  IDNo: " + IDNo
                        + "  ExpiryDate: " + (xPiry == String.Empty ? null : xPiry)
                        + "  ClaimedDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + "  SODate: " + SODate
                        + "  syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + "  BranchCode: " + BranchCode
                        + "  ZoneCode: " + ZoneCode
                        + "  Balance: " + balance
                        + "  DormantCharge: " + DormantCharge
                        + "  CustID: " + senderid
                        + "  SenderMLCardNO: " + SenderMLCardNO
                        + "  SenderFName: " + SenderFName
                        + "  SenderLName: " + SenderLName
                        + "  SenderMName: " + SenderMName
                        + "  SenderName: " + SenderLName + ", " + SenderFName + " " + SenderMName
                        + "  SenderStreet: " + SenderStreet
                        + "  SenderProvinceCity: " + SenderProvinceCity
                        + "  SenderCountry: " + SenderCountry
                        + "  SenderGender: " + SenderGender
                        + "  SenderContactNo: " + SenderContactNo
                        + "  SenderIsSMS: " + SenderIsSMS
                        + "  SenderBirthdate: " + sBdate
                        + "  SenderBranchID: " + SenderBranchID
                        + "  ReceiverMLCardNO: " + ReceiverMLCardNO
                        + "  ReceiverFName: " + ReceiverFName
                        + "  ReceiverLName: " + ReceiverLName
                        + "  ReceiverMName: " + ReceiverMName
                        + "  ReceiverName: " + ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName
                        + "  ReceiverStreet: " + ReceiverStreet
                        + "  ReceiverProvinceCity: " + ReceiverProvinceCity
                        + "  ReceiverCountry: " + ReceiverCountry
                        + "  ReceiverGender: " + ReceiverGender
                        + "  ReceiverContactNo: " + ReceiverContactNo
                        + "  ReceiverBirthdate: " + rBdate
                        + "  SOORNo: " + ORNo
                        + "  IRNo: " + irno
                        + "  CancelledByOperatorID: " + CancelledByOperatorID
                        + "  CancelledByStationID: " + CancelledByStationID
                        + "  CancelledType: " + CancelledType
                        + "  CancelledReason: " + CancelReason
                        + "  CancelledCustCharge: " + CancelCharge
                        + "  CancelledByBranchCode: " + CancelledByBranchCode
                        + "  transtp: " + transtype);


                        command.CommandText = "update " + generateTableNameGlobal(0, null) + " set IsClaimed = 1, sysmodified = @modified, sysmodifier = @modifier where KPTNNo = @kptn";
                        command.Parameters.AddWithValue("modified", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("modifier", sysmodifier);
                        command.Parameters.AddWithValue("kptn", KPTNNo);
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message : update " + generateTableNameGlobal(0, null) + ":  IsClaimed: 1 " + ", sysmodified: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + ", sysmodifier: " + sysmodifier + ", KPTNNo: " + KPTNNo);



                        command.CommandText = "update " + generateTableNameGlobal(2, null) + " set IsClaimed = 1, DateClaimed = @dtClaimed where KPTN6 = @kptn1 OR MLKP4TN = @kptn1";
                        command.Parameters.AddWithValue("kptn1", KPTNNo);
                        command.Parameters.AddWithValue("dtClaimed", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCES:: update " + generateTableNameGlobal(2, null) + " IsClaimed : 1, DateClaimed:  " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " KPTN6: " + KPTNNo);

                        //command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and userid = @uid and zcode = @zcode and type = 1";
                        //command.Parameters.AddWithValue("bcode", BranchCode);
                        //command.Parameters.AddWithValue("uid", OperatorID);
                        //command.Parameters.AddWithValue("series", sr);
                        //command.Parameters.AddWithValue("zcode", ZoneCode);

                        //throw new Exception(IsRemote.ToString() + " " + series);
                        if (IsRemote == 1)
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "01");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", ZoneCode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                        }
                        else
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", Station);
                            command.Parameters.AddWithValue("bcode", BranchCode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", ZoneCode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                            kplog.Info("SUCCES:: message : update kpformsglobal.control:  "
                         + "  st: " + Station
                         + "  bcode: " + BranchCode
                         + "  series: " + (sr + 1)
                         + "  zcode: " + ZoneCode
                         + "  tp: " + type);
                        }


                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1,  message : " + getRespMessage(1) + ", DateClaimed: " + dt);
                        return new PayoutResponse { respcode = 1, message = getRespMessage(1), DateClaimed = dt };
                    }
                }
                catch (MySqlException ex)
                {
                    kplog.Fatal(ex.ToString());
                    Int32 respcode = 0;
                    //String message;
                    //throw new Exception(ex.ErrorCode.ToString());
                    if (ex.Message.Contains("Duplicate"))
                    {
                        respcode = 3;
                        kplog.Fatal("FAILED:: respcode: 3, message: " + getRespMessage(3));
                    }
                    trans.Rollback();
                    dbconGlobal.CloseConnection();
                    kplog.Fatal("FAILED:: respcode: " + respcode + ", message : " + getRespMessage(respcode) + " , ErrorDetail: " + ex.ToString() + " , DateClaimed: " + DateTime.Now);
                    return new PayoutResponse { respcode = respcode, message = getRespMessage(respcode), ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
                }
            }
        }
        catch (Exception ex)
        {
          //  kplog.Fatal(ex.ToString());
            //trans.Rollback();
            dbconGlobal.CloseConnection();
            kplog.Fatal("FAILED:: respcode: 0 , message : " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", DateClaimed: " + DateTime.Now);
            return new PayoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), DateClaimed = DateTime.Now };
        }


    }


    [WebMethod]
    public ChargeResponse getPromoListGlobal(String Username, String Password)
    {
        kplog.Info("Username: "+Username+", Password: "+Password);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {

                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "select promoname, effectivedate, expirydate, NOW() as now from kpformsglobal.promoratesheader WHERE DATE_FORMAT(NOW(),'%Y-%m-%d') <= DATE_FORMAT(expirydate,'%Y-%m-%d') order by effectivedate;";

                        command.CommandText = query;
                        //List<string> promos = new List<string>();

                        MySqlDataReader ReaderCount = command.ExecuteReader();
                        int arraysize = 0;
                        if (ReaderCount.HasRows)
                        {
                            while (ReaderCount.Read())
                            {
                                arraysize = arraysize + 1;
                            }
                            ReaderCount.Close();
                            MySqlDataReader Reader = command.ExecuteReader();
                            PromoList[] promoList = new PromoList[arraysize];
                            int arrayCounter = 0;

                            while (Reader.Read())
                            {
                                promoList[arrayCounter] = new PromoList { promoName = Reader["promoname"].ToString(), effectiveDate = Reader["effectivedate"].ToString(), expiryDate = Reader["expirydate"].ToString(), active = verifyValidity(Convert.ToDateTime(Reader["now"]), Convert.ToDateTime(Reader["expirydate"])) };
                                arrayCounter = arrayCounter + 1;
                            }

                            Reader.Close();
                            conn.Close();
                            //throw new Exception(arrayCounter.ToString());
                            kplog.Info("SUCCESS:: message: " + getRespMessage(1) + "promos: " + promoList);
                            return new ChargeResponse { respcode = 1, message = getRespMessage(1), promos = promoList };
                        }
                        else
                        {
                            ReaderCount.Close();
                            conn.Close();
                            kplog.Error("FAILED:: respcode: 16 , message : No Promo Available, Charge: " + dec);
                            return new ChargeResponse { respcode = 16, message = "No promo available.", charge = dec };
                        }
                        //trans.Commit();

                        //return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal(ex.ToString());
                trans.Rollback();
                conn.Close();
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }


    [WebMethod]
    public ChargeResponse getPromoListDomestic(String Username, String Password)
    {
        kplog.Info("Username: "+Username+", Password: "+Password);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            try
            {

                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "select promoname, effectivedate, expirydate, NOW() as now from kpforms.promoratesheader WHERE DATE_FORMAT(NOW(),'%Y-%m-%d') <= DATE_FORMAT(expirydate,'%Y-%m-%d') order by effectivedate;";

                        command.CommandText = query;
                        //List<string> promos = new List<string>();

                        MySqlDataReader ReaderCount = command.ExecuteReader();
                        int arraysize = 0;
                        if (ReaderCount.HasRows)
                        {
                            while (ReaderCount.Read())
                            {
                                arraysize = arraysize + 1;
                            }
                            ReaderCount.Close();
                            MySqlDataReader Reader = command.ExecuteReader();
                            PromoList[] promoList = new PromoList[arraysize];
                            int arrayCounter = 0;

                            while (Reader.Read())
                            {
                                promoList[arrayCounter] = new PromoList { promoName = Reader["promoname"].ToString(), effectiveDate = Reader["effectivedate"].ToString(), expiryDate = Reader["expirydate"].ToString(), active = verifyValidity(Convert.ToDateTime(Reader["now"]), Convert.ToDateTime(Reader["expirydate"])) };
                                arrayCounter = arrayCounter + 1;
                            }

                            Reader.Close();
                            conn.Close();
                            //throw new Exception(arrayCounter.ToString());
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", promos: " + promoList);
                            return new ChargeResponse { respcode = 1, message = getRespMessage(1), promos = promoList };
                        }
                        else
                        {
                            ReaderCount.Close();
                            conn.Close();
                            kplog.Error("FAILED:: respcode: 16, message : No Promo Available , Charge: " + dec);
                            return new ChargeResponse { respcode = 16, message = "No promo available.", charge = dec };
                        }
                        //trans.Commit();

                        //return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                //kplog.Fatal(ex.ToString());
                trans.Rollback();
                conn.Close();
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }


    [WebMethod]
    public ChargeResponse getPromoChargeGlobalUS(String Username, String Password, String promoname, Decimal promoamount)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", promoname: " + promoname + ", promoamount: " + promoamount);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {

                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "select currid from kpformsglobal.promoratesheaderG where promoname = @promoname;";
                        command.CommandText = query;
                        command.Parameters.AddWithValue("promoname", promoname);


                        //List<string> promos = new List<string>();

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            // int arraysize = 0;
                            if (reader.HasRows)
                            {
                                reader.Read();
                                Int32 currid = Convert.ToInt32(reader["currid"]);
                                reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.promorateschargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;

                                command.Parameters.AddWithValue("type", currid);
                                command.Parameters.AddWithValue("amount", promoamount);
                                using (MySqlDataReader readerCharges = command.ExecuteReader())
                                {
                                    if (readerCharges.HasRows)
                                    {
                                        readerCharges.Read();

                                        decimal charge = Convert.ToDecimal(readerCharges["charge"]);

                                        readerCharges.Close();
                                        conn.Close();
                                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + " , Charge: " + charge);
                                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = charge };
                                    }
                                    else
                                    {
                                        readerCharges.Close();
                                        conn.Close();
                                        kplog.Error("FAILED:: respcode: 0, message : No Rates Found!");
                                        return new ChargeResponse { respcode = 0, message = "No rates found." };
                                    }

                                }
                                //conn.Close();
                                //throw new Exception(arrayCounter.ToString());
                            }
                            else
                            {
                                reader.Close();
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 16, message: Promo not found, Charge: " + dec);
                                return new ChargeResponse { respcode = 16, message = "Promo not found.", charge = dec };
                            }
                        }
                        //trans.Commit();

                        //return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public ChargeResponse getPromoChargeGlobal(String Username, String Password, String promoname, Decimal promoamount)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", promoname: " + promoname + ", promoamount: " + promoamount);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {

                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "select currid from kpformsglobal.promoratesheader where promoname = @promoname;";
                        command.CommandText = query;
                        command.Parameters.AddWithValue("promoname", promoname);


                        //List<string> promos = new List<string>();

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            // int arraysize = 0;
                            if (reader.HasRows)
                            {
                                reader.Read();
                                Int32 currid = Convert.ToInt32(reader["currid"]);
                                reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.promoratescharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;

                                command.Parameters.AddWithValue("type", currid);
                                command.Parameters.AddWithValue("amount", promoamount);
                                using (MySqlDataReader readerCharges = command.ExecuteReader())
                                {
                                    if (readerCharges.HasRows)
                                    {
                                        readerCharges.Read();

                                        decimal charge = Convert.ToDecimal(readerCharges["charge"]);

                                        readerCharges.Close();
                                        conn.Close();
                                        kplog.Info("SUCCESS:: respcode: 1 , message : " + getRespMessage(1) + ", Charge: " + charge);
                                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = charge };
                                    }
                                    else
                                    {
                                        readerCharges.Close();
                                        conn.Close();
                                        kplog.Error("FAILED:: respcode: 0 , message : No Rates Found!");
                                        return new ChargeResponse { respcode = 0, message = "No rates found." };
                                    }

                                }
                                //conn.Close();
                                //throw new Exception(arrayCounter.ToString());
                            }
                            else
                            {
                                reader.Close();
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 16 , message : Promo not Found! , Charge: " + dec);
                                return new ChargeResponse { respcode = 16, message = "Promo not found.", charge = dec };
                            }
                        }
                        //trans.Commit();

                        //return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
             //   kplog.Fatal(ex.ToString());
                trans.Rollback();
                conn.Close();
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public ChargeResponse getPromoChargeDomestic(String Username, String Password, String promoname, Decimal promoamount)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", promoname: " + promoname + ", promoamount: " + promoamount);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            try
            {

                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "select currid from kpforms.promoratesheader where promoname = @promoname;";
                        command.CommandText = query;
                        command.Parameters.AddWithValue("promoname", promoname);


                        //List<string> promos = new List<string>();

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            // int arraysize = 0;
                            if (reader.HasRows)
                            {
                                reader.Read();
                                Int32 currid = Convert.ToInt32(reader["currid"]);
                                reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpforms.promoratescharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;

                                command.Parameters.AddWithValue("type", currid);
                                command.Parameters.AddWithValue("amount", promoamount);
                                using (MySqlDataReader readerCharges = command.ExecuteReader())
                                {
                                    if (readerCharges.HasRows)
                                    {
                                        readerCharges.Read();

                                        decimal charge = Convert.ToDecimal(readerCharges["charge"]);

                                        readerCharges.Close();
                                        conn.Close();
                                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", Charge: " + charge);
                                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = charge };
                                    }
                                    else
                                    {
                                        readerCharges.Close();
                                        conn.Close();
                                        kplog.Error("FAILED:: respcode: 0 , message: No Rates Found!");
                                        return new ChargeResponse { respcode = 0, message = "No rates found." };
                                    }

                                }
                                //conn.Close();
                                //throw new Exception(arrayCounter.ToString());
                            }
                            else
                            {
                                reader.Close();
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 16, message : Promo Not Found! , Charge: " + dec);
                                return new ChargeResponse { respcode = 16, message = "Promo not found.", charge = dec };
                            }
                        }
                        //trans.Commit();

                        //return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public ChargeResponse calculateChargePerBranchGlobalUS(String Username, String Password, Double amount, String bcode, String zcode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", amount: " + amount + ", bcode: " + bcode + ", zcode: " + zcode + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "SELECT nextID,currID,nDateEffectivity,cDateEffectivity,cEffective,nextID, NOW() as currentDate FROM kpformsglobal.ratesperbranchheaderG WHERE cEffective = 1 and branchcode = @bcode and zonecode = @zcode;";

                        command.CommandText = query;
                        command.Parameters.AddWithValue("bcode", bcode);
                        command.Parameters.AddWithValue("zcode", zcode);
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.Read())
                        {
                            Int32 nextID = Convert.ToInt32(Reader["nextID"]);
                            Int32 type = Convert.ToInt32(Reader["currID"]);
                            //String ndate = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? null : Convert.ToDateTime(Reader["nDateEffectivity"]).ToString();
                            DateTime nDateEffectivity = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? NullDate : Convert.ToDateTime(Reader["nDateEffectivity"]);
                            DateTime currentDate = Convert.ToDateTime(Reader["currentDate"]);
                            //throw new Exception(nDateEffectivity.ToString());
                            if (nextID == 0)
                            {
                                Reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.ratesperbranchchargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;
                                command.Parameters.AddWithValue("amount", amount);
                                command.Parameters.AddWithValue("type", type);

                                MySqlDataReader ReaderRates = command.ExecuteReader();
                                if (ReaderRates.Read())
                                {
                                    dec = (Decimal)ReaderRates["charge"];
                                    ReaderRates.Close();
                                }
                            }
                            else
                            {
                                Reader.Close();

                                int result = DateTime.Compare(nDateEffectivity, currentDate);

                                if (result < 0)
                                {

                                    //ReaderNextRates.Close();
                                    //UPDATE ANG TABLE EFFECTIVE
                                    // 0 = pending, 1 = current chage, 2 = unused

                                    //try
                                    //{
                                    command.Transaction = trans;
                                    command.Parameters.Clear();
                                    String updateRates = "update kpformsglobal.ratesperbranchheaderG SET  cEffective = 2 where cEffective = 1 and branchcode = @bcode and zonecode = @zcode";
                                    command.CommandText = updateRates;
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("zcode", zcode);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCES:: message: update kpformsglobal.ratesperbranchheaderG: cEffective : 2 , branchcode: " + bcode + ", zonecode: " + zcode);

                                    command.Parameters.Clear();
                                    String updateRates1 = "update kpformsglobal.ratesperbranchheaderG SET cEffective = 1 where currID = @curr and branchcode = @bcode and zonecode = @zcode";
                                    command.CommandText = updateRates1;
                                    command.Parameters.AddWithValue("curr", nextID);
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("zcode", zcode);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : update kpformsglobal.ratesperbranchheaderG: cEffevtive: 1 , currID: " + nextID + ",branchcode: " + bcode + ", zonecode: " + zcode);

                                    command.Parameters.Clear();
                                    String insertLog = "insert into kpadminlogsglobal.kpratesupdatelogs (ModifiedRatesID, NewRatesID, DateModified, Modifier) values (@ModifiedRatesID, @NewRatesID, NOW(), @Modifier);";
                                    command.CommandText = insertLog;
                                    command.Parameters.AddWithValue("ModifiedRatesID", nextID - 1);
                                    command.Parameters.AddWithValue("NewRatesID", nextID);
                                    command.Parameters.AddWithValue("Modifier", "boskpws");
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : insert into kpadminlogsglobal.kpratesupdatelogs: "
                                  + " ModifiedRatesID: " + (nextID - 1)
                                  + " NewRatesID: " + nextID
                                  + " Modifier: " + "boskpws");

                                    trans.Commit();

                                    //}catch(MySqlException ex){
                                    //    //trans.Rollback();
                                    //    Reader.Close();

                                    //    throw new Exception(ex.ToString());
                                    //}

                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.ratesperbranchchargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", nextID);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                                else
                                {
                                    //ReaderNextRates.Close();


                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.ratesperbranchchargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", type);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                            }


                        }
                        else
                        {
                            kplog.Fatal(getRespMessage(16));
                            Reader.Close();
                            conn.Close();
                            kplog.Fatal("FAILED:: respcode: 16, message : " + getRespMessage(16) + ", Charge: " + dec);
                            return new ChargeResponse { respcode = 16, message = getRespMessage(16), charge = dec };
                        }
                        //trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", Charge:  " + dec);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        trans.Rollback();
                        conn.Close();
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public ChargeResponse calculateChargePerBranchGlobal(String Username, String Password, Double amount, String bcode, String zcode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", amount: " + amount + ", bcode: " + bcode + ", zcode: " + zcode + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "SELECT nextID,currID,nDateEffectivity,cDateEffectivity,cEffective,nextID, NOW() as currentDate FROM kpformsglobal.ratesperbranchheader WHERE cEffective = 1 and branchcode = @bcode and zonecode = @zcode;";

                        command.CommandText = query;
                        command.Parameters.AddWithValue("bcode", bcode);
                        command.Parameters.AddWithValue("zcode", zcode);
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.Read())
                        {
                            Int32 nextID = Convert.ToInt32(Reader["nextID"]);
                            Int32 type = Convert.ToInt32(Reader["currID"]);
                            //String ndate = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? null : Convert.ToDateTime(Reader["nDateEffectivity"]).ToString();
                            DateTime nDateEffectivity = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? NullDate : Convert.ToDateTime(Reader["nDateEffectivity"]);
                            DateTime currentDate = Convert.ToDateTime(Reader["currentDate"]);
                            //throw new Exception(nDateEffectivity.ToString());
                            if (nextID == 0)
                            {
                                Reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.ratesperbranchcharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;
                                command.Parameters.AddWithValue("amount", amount);
                                command.Parameters.AddWithValue("type", type);

                                MySqlDataReader ReaderRates = command.ExecuteReader();
                                if (ReaderRates.Read())
                                {
                                    dec = (Decimal)ReaderRates["charge"];
                                    ReaderRates.Close();
                                }
                            }
                            else
                            {
                                Reader.Close();

                                int result = DateTime.Compare(nDateEffectivity, currentDate);

                                if (result < 0)
                                {

                                    //ReaderNextRates.Close();
                                    //UPDATE ANG TABLE EFFECTIVE
                                    // 0 = pending, 1 = current chage, 2 = unused

                                    //try
                                    //{
                                    command.Transaction = trans;
                                    command.Parameters.Clear();
                                    String updateRates = "update kpformsglobal.ratesperbranchheader SET  cEffective = 2 where cEffective = 1 and branchcode = @bcode and zonecode = @zcode";
                                    command.CommandText = updateRates;
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("zcode", zcode);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS: message: update kpformsglobal.ratesperbranchheader: cEffective: 2 , branchcode: " + bcode + ", zonecode: " + zcode);

                                    command.Parameters.Clear();
                                    String updateRates1 = "update kpformsglobal.ratesperbranchheader SET cEffective = 1 where currID = @curr and branchcode = @bcode and zonecode = @zcode";
                                    command.CommandText = updateRates1;
                                    command.Parameters.AddWithValue("curr", nextID);
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("zcode", zcode);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.ratesperbranchheader: cEffective: 1 , currID: " + nextID + ", branchcode: " + bcode + ", zonecode: " + zcode);
                                   
                                    command.Parameters.Clear();
                                    String insertLog = "insert into kpadminlogsglobal.kpratesupdatelogs (ModifiedRatesID, NewRatesID, DateModified, Modifier) values (@ModifiedRatesID, @NewRatesID, NOW(), @Modifier);";
                                    command.CommandText = insertLog;
                                    command.Parameters.AddWithValue("ModifiedRatesID", nextID - 1);
                                    command.Parameters.AddWithValue("NewRatesID", nextID);
                                    command.Parameters.AddWithValue("Modifier", "boskpws");
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : insert into kpadminlogsglobal.kpratesupdatelogs: "
                                    + " ModifiedRatesID: " + (nextID - 1)
                                    + " NewRatesID: " + nextID
                                    + " Modifier: " + "boskpws");

                                    trans.Commit();

                                    //}catch(MySqlException ex){
                                    //    //trans.Rollback();
                                    //    Reader.Close();

                                    //    throw new Exception(ex.ToString());
                                    //}

                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.ratesperbranchcharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", nextID);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                                else
                                {
                                    //ReaderNextRates.Close();


                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.ratesperbranchcharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", type);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                            }


                        }
                        else
                        {
                            kplog.Fatal(getRespMessage(16));
                            Reader.Close();
                            conn.Close();
                            kplog.Error("FAILED:: respcode: 16, message: " + getRespMessage(16) + ", charge: " + dec);
                            return new ChargeResponse { respcode = 16, message = getRespMessage(16), charge = dec };
                        }
                        //trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message : " + getRespMessage(1) + " charge: " + dec);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        kplog.Fatal(mex.ToString());
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }


    [WebMethod]
    public ChargeResponse calculateChargePerBranchDomestic(String Username, String Password, Double amount, String bcode, String zcode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", amount: " + amount + ", bcode: " + bcode + ", zcode: " + zcode + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "SELECT nextID,currID,nDateEffectivity,cDateEffectivity,cEffective,nextID, NOW() as currentDate FROM kpforms.ratesperbranchheader WHERE cEffective = 1 and branchcode = @bcode and zonecode = @zcode;";

                        command.CommandText = query;
                        command.Parameters.AddWithValue("bcode", bcode);
                        command.Parameters.AddWithValue("zcode", zcode);
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.Read())
                        {
                            Int32 nextID = Convert.ToInt32(Reader["nextID"]);
                            Int32 type = Convert.ToInt32(Reader["currID"]);
                            //String ndate = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? null : Convert.ToDateTime(Reader["nDateEffectivity"]).ToString();
                            DateTime nDateEffectivity = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? NullDate : Convert.ToDateTime(Reader["nDateEffectivity"]);
                            DateTime currentDate = Convert.ToDateTime(Reader["currentDate"]);
                            //throw new Exception(nDateEffectivity.ToString());
                            if (nextID == 0)
                            {
                                Reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpforms.ratesperbranchcharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;
                                command.Parameters.AddWithValue("amount", amount);
                                command.Parameters.AddWithValue("type", type);

                                MySqlDataReader ReaderRates = command.ExecuteReader();
                                if (ReaderRates.Read())
                                {
                                    dec = (Decimal)ReaderRates["charge"];
                                    ReaderRates.Close();
                                }
                            }
                            else
                            {
                                Reader.Close();

                                int result = DateTime.Compare(nDateEffectivity, currentDate);

                                if (result < 0)
                                {

                                    //ReaderNextRates.Close();
                                    //UPDATE ANG TABLE EFFECTIVE
                                    // 0 = pending, 1 = current chage, 2 = unused

                                    //try
                                    //{
                                    command.Transaction = trans;
                                    command.Parameters.Clear();
                                    String updateRates = "update kpforms.ratesperbranchheader SET  cEffective = 2 where cEffective = 1 and branchcode = @bcode and zonecode = @zcode";
                                    command.CommandText = updateRates;
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("zcode", zcode);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpforms.ratesperbranchheader: cEffective : 2 , branchcode:  " + bcode + ", zonecode: " + zcode);

                                    command.Parameters.Clear();
                                    String updateRates1 = "update kpforms.ratesperbranchheader SET cEffective = 1 where currID = @curr and branchcode = @bcode and zonecode = @zcode";
                                    command.CommandText = updateRates1;
                                    command.Parameters.AddWithValue("curr", nextID);
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("zcode", zcode);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : update kpforms.ratesperbranchheader: cEffective: 1 , currID : " + nextID + " branchcode: " + bcode + ", zonecode: " + zcode);

                                    command.Parameters.Clear();
                                    String insertLog = "insert into kpadminlogs.kpratesupdatelogs (ModifiedRatesID, NewRatesID, DateModified, Modifier) values (@ModifiedRatesID, @NewRatesID, NOW(), @Modifier);";
                                    command.CommandText = insertLog;
                                    command.Parameters.AddWithValue("ModifiedRatesID", nextID - 1);
                                    command.Parameters.AddWithValue("NewRatesID", nextID);
                                    command.Parameters.AddWithValue("Modifier", "boskpws");
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : insert into kpadminlogs.kpratesupdatelogs: "
                                     + " ModifiedRatesID: " + (nextID - 1)
                                     + " NewRatesID: " + nextID
                                     + " Modifier: " + "boskpws");


                                    trans.Commit();

                                    //}catch(MySqlException ex){
                                    //    //trans.Rollback();
                                    //    Reader.Close();

                                    //    throw new Exception(ex.ToString());
                                    //}

                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpforms.ratesperbranchcharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", nextID);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                                else
                                {
                                    //ReaderNextRates.Close();


                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpforms.ratesperbranchcharges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", type);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                            }


                        }
                        else
                        {
                            kplog.Error("FAILED:: respcode: 16, message : " + getRespMessage(16) + ", Charge: " + dec);
                            Reader.Close();
                            conn.Close();
                            return new ChargeResponse { respcode = 16, message = getRespMessage(16), charge = dec };
                        }
                        //trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", charge: " + dec);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };
                    }
                    catch (MySqlException mex)
                    {
                        kplog.Fatal(mex.ToString());
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public ChargeResponse calculateChargeGlobalUS(String Username, String Password, Double amount, String bcode, String zcode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", amount: " + amount + ", bcode: " + bcode + ", zcode: " + zcode + ", version: " + version + ", stationcode: " + stationcode);


        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "SELECT nextID,currID,nDateEffectivity,cDateEffectivity,cEffective,nextID, NOW() as currentDate FROM kpformsglobal.headerchargesG WHERE cEffective = 1;";

                        command.CommandText = query;
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.Read())
                        {
                            Int32 nextID = Convert.ToInt32(Reader["nextID"]);
                            Int32 type = Convert.ToInt32(Reader["currID"]);
                            //String ndate = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? null : Convert.ToDateTime(Reader["nDateEffectivity"]).ToString();
                            DateTime nDateEffectivity = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? NullDate : Convert.ToDateTime(Reader["nDateEffectivity"]);
                            DateTime currentDate = Convert.ToDateTime(Reader["currentDate"]);
                            //throw new Exception(nDateEffectivity.ToString());
                            if (nextID == 0)
                            {
                                //calculatecharge_search(IN amount DECIMAL(10,2), IN `type` VARCHAR(15))
                                Reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.chargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;
                                command.Parameters.AddWithValue("amount", amount);
                                command.Parameters.AddWithValue("type", type);

                                MySqlDataReader ReaderRates = command.ExecuteReader();
                                if (ReaderRates.Read())
                                {
                                    dec = (Decimal)ReaderRates["charge"];
                                    ReaderRates.Close();
                                }
                            }
                            else
                            {
                                Reader.Close();

                                int result = DateTime.Compare(nDateEffectivity, currentDate);

                                if (result < 0)
                                {

                                    //ReaderNextRates.Close();
                                    //UPDATE ANG TABLE EFFECTIVE
                                    // 0 = pending, 1 = current chage, 2 = unused

                                    //try
                                    //{

                                    command.Transaction = trans;
                                    command.Parameters.Clear();
                                    String updateRates = "update kpformsglobal.headerchargesG SET  cEffective = 2 where cEffective = 1";
                                    command.CommandText = updateRates;
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.headerchargesG: cEffective: 2");

                                    command.Parameters.Clear();
                                    String updateRates1 = "update kpformsglobal.headerchargesG SET cEffective = 1 where currID = @curr";
                                    command.CommandText = updateRates1;
                                    command.Parameters.AddWithValue("curr", nextID);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.headerchargesG : cEffective: 1, currID: " + nextID);


                                    command.Parameters.Clear();
                                    String insertLog = "insert into kpadminlogsglobal.kpratesupdatelogs (ModifiedRatesID, NewRatesID, DateModified, Modifier) values (@ModifiedRatesID, @NewRatesID, NOW(), @Modifier);";
                                    command.CommandText = insertLog;
                                    command.Parameters.AddWithValue("ModifiedRatesID", nextID - 1);
                                    command.Parameters.AddWithValue("NewRatesID", nextID);
                                    command.Parameters.AddWithValue("Modifier", "boskpws");
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : insert into kpadminlogsglobal.kpratesupdatelogs: "
                                     + " ModifiedRatesID: " + (nextID - 1)
                                     + " NewRatesID: " + nextID
                                     + " Modifier: " + "boskpws");
                                    trans.Commit();
                                    //}catch(MySqlException ex){
                                    //    //trans.Rollback();
                                    //    Reader.Close();

                                    //    throw new Exception(ex.ToString());
                                    //}

                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.chargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", nextID);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                                else
                                {
                                    //ReaderNextRates.Close();


                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.chargesG WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", type);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                            }


                        }
                        //trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1, message: " + getRespMessage(1) + ", charge: " + dec);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };


                    }
                    catch (MySqlException mex)
                    {
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        trans.Rollback();
                        conn.Close();
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod]
    public ChargeResponse calculateChargeGlobal(String Username, String Password, Double amount, String bcode, String zcode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", amount: " + amount + ", bcode: " + bcode + ", zcode: " + zcode + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "SELECT nextID,currID,nDateEffectivity,cDateEffectivity,cEffective,nextID, NOW() as currentDate FROM kpformsglobal.headercharges WHERE cEffective = 1;";

                        command.CommandText = query;
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.Read())
                        {
                            Int32 nextID = Convert.ToInt32(Reader["nextID"]);
                            Int32 type = Convert.ToInt32(Reader["currID"]);
                            //String ndate = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? null : Convert.ToDateTime(Reader["nDateEffectivity"]).ToString();
                            DateTime nDateEffectivity = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? NullDate : Convert.ToDateTime(Reader["nDateEffectivity"]);
                            DateTime currentDate = Convert.ToDateTime(Reader["currentDate"]);
                            //throw new Exception(nDateEffectivity.ToString());
                            if (nextID == 0)
                            {
                                //calculatecharge_search(IN amount DECIMAL(10,2), IN `type` VARCHAR(15))
                                Reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.charges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;
                                command.Parameters.AddWithValue("amount", amount);
                                command.Parameters.AddWithValue("type", type);

                                MySqlDataReader ReaderRates = command.ExecuteReader();
                                if (ReaderRates.Read())
                                {
                                    dec = (Decimal)ReaderRates["charge"];
                                    ReaderRates.Close();
                                }
                            }
                            else
                            {
                                Reader.Close();

                                int result = DateTime.Compare(nDateEffectivity, currentDate);

                                if (result < 0)
                                {

                                    //ReaderNextRates.Close();
                                    //UPDATE ANG TABLE EFFECTIVE
                                    // 0 = pending, 1 = current chage, 2 = unused

                                    //try
                                    //{

                                    command.Transaction = trans;
                                    command.Parameters.Clear();
                                    String updateRates = "update kpformsglobal.headercharges SET  cEffective = 2 where cEffective = 1";
                                    command.CommandText = updateRates;
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.headercharges : cEffective: 2");

                                    command.Parameters.Clear();
                                    String updateRates1 = "update kpformsglobal.headercharges SET cEffective = 1 where currID = @curr";
                                    command.CommandText = updateRates1;
                                    command.Parameters.AddWithValue("curr", nextID);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: updatekpformsglobal.headercharges: cEffective: 1, currID: " + nextID);

                                    command.Parameters.Clear();
                                    String insertLog = "insert into kpadminlogsglobal.kpratesupdatelogs (ModifiedRatesID, NewRatesID, DateModified, Modifier) values (@ModifiedRatesID, @NewRatesID, NOW(), @Modifier);";
                                    command.CommandText = insertLog;
                                    command.Parameters.AddWithValue("ModifiedRatesID", nextID - 1);
                                    command.Parameters.AddWithValue("NewRatesID", nextID);
                                    command.Parameters.AddWithValue("Modifier", "boskpws");
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: insert into kpadminlogsglobal.kpratesupdatelogs: "
                                     + " ModifiedRatesID: " + (nextID - 1)
                                     + " NewRatesID: " + nextID
                                     + " Modifier: " + "boskpws");
                                    trans.Commit();
                                    //}catch(MySqlException ex){
                                    //    //trans.Rollback();
                                    //    Reader.Close();

                                    //    throw new Exception(ex.ToString());
                                    //}

                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.charges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", nextID);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                                else
                                {
                                    //ReaderNextRates.Close();


                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpformsglobal.charges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", type);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                            }


                        }
                        //trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1, message: " + getRespMessage(1) + ", charge: " + dec);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };


                    }
                    catch (MySqlException mex)
                    {
                        kplog.Fatal(mex.ToString());
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }


    [WebMethod]
    public ChargeResponse calculateChargeDomestic(String Username, String Password, Double amount, String bcode, String zcode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", amount: " + amount + ", bcode: " + bcode + ", zcode: " + zcode + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ChargeResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {

                    DateTime NullDate = DateTime.MinValue;

                    Decimal dec = 0;
                    conn.Open();
                    trans = conn.BeginTransaction();

                    try
                    {
                        String query = "SELECT nextID,currID,nDateEffectivity,cDateEffectivity,cEffective,nextID, NOW() as currentDate FROM kpforms.headercharges WHERE cEffective = 1;";

                        command.CommandText = query;
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.Read())
                        {
                            Int32 nextID = Convert.ToInt32(Reader["nextID"]);
                            Int32 type = Convert.ToInt32(Reader["currID"]);
                            //String ndate = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? null : Convert.ToDateTime(Reader["nDateEffectivity"]).ToString();
                            DateTime nDateEffectivity = (Reader["nDateEffectivity"].ToString().StartsWith("0")) ? NullDate : Convert.ToDateTime(Reader["nDateEffectivity"]);
                            DateTime currentDate = Convert.ToDateTime(Reader["currentDate"]);
                            //throw new Exception(nDateEffectivity.ToString());
                            if (nextID == 0)
                            {
                                Reader.Close();
                                String queryRates = "SELECT ChargeValue AS charge FROM kpforms.charges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                command.CommandText = queryRates;
                                command.Parameters.AddWithValue("amount", amount);
                                command.Parameters.AddWithValue("type", type);

                                MySqlDataReader ReaderRates = command.ExecuteReader();
                                if (ReaderRates.Read())
                                {
                                    dec = (Decimal)ReaderRates["charge"];
                                    ReaderRates.Close();
                                }
                            }
                            else
                            {
                                Reader.Close();

                                int result = DateTime.Compare(nDateEffectivity, currentDate);

                                if (result < 0)
                                {

                                    //ReaderNextRates.Close();
                                    //UPDATE ANG TABLE EFFECTIVE
                                    // 0 = pending, 1 = current chage, 2 = unused

                                    //try
                                    //{
                                    command.Transaction = trans;
                                    command.Parameters.Clear();
                                    String updateRates = "update kpforms.headercharges SET  cEffective = 2 where cEffective = 1";
                                    command.CommandText = updateRates;
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpforms.headercharges : cEffective: 2");

                                    command.Parameters.Clear();
                                    String updateRates1 = "update kpforms.headercharges SET cEffective = 1 where currID = @curr";
                                    command.CommandText = updateRates1;
                                    command.Parameters.AddWithValue("curr", nextID);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : update kpforms.headercharges : cEffective: 1 , currID: " + nextID);

                                    command.Parameters.Clear();
                                    String insertLog = "insert into kpadminlogs.kpratesupdatelogs (ModifiedRatesID, NewRatesID, DateModified, Modifier) values (@ModifiedRatesID, @NewRatesID, NOW(), @Modifier);";
                                    command.CommandText = insertLog;
                                    command.Parameters.AddWithValue("ModifiedRatesID", nextID - 1);
                                    command.Parameters.AddWithValue("NewRatesID", nextID);
                                    command.Parameters.AddWithValue("Modifier", "boskpws");
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: INSERT INTO kpadminlogs.kpratesupdatelogs"
                                    + " ModifiedRatesID: " +( nextID - 1)
                                     + " NewRatesID: " + nextID
                                     + " Modifier: " + "boskpws");
                                    trans.Commit();
                                    //}catch(MySqlException ex){
                                    //    //trans.Rollback();
                                    //    Reader.Close();

                                    //    throw new Exception(ex.ToString());
                                    //}

                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpforms.charges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", nextID);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                                else
                                {
                                    //ReaderNextRates.Close();


                                    command.Parameters.Clear();
                                    String queryRates = "SELECT ChargeValue AS charge FROM kpforms.charges WHERE ROUND(@amount,2) BETWEEN MinAmount AND MaxAmount AND `type` = @type;";
                                    command.CommandText = queryRates;
                                    command.Parameters.AddWithValue("amount", amount);
                                    command.Parameters.AddWithValue("type", type);

                                    MySqlDataReader ReaderRates = command.ExecuteReader();
                                    if (ReaderRates.Read())
                                    {
                                        //ReaderRates.Read();
                                        dec = (Decimal)ReaderRates["charge"];
                                        ReaderRates.Close();
                                    }
                                }
                            }


                        }
                        //trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respocode: 1 , message : " + getRespMessage(1) + ", Charge: " + dec);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), charge = dec };


                    }
                    catch (MySqlException mex)
                    {
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + mex.ToString());
                        trans.Rollback();
                        conn.Close();
                        return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = mex.ToString() };
                    }
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                trans.Rollback();
                conn.Close();
                return new ChargeResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }
    }

    [WebMethod(BufferResponse = false)]
    public SendoutResponse sendoutGlobal(String Username, String Password, List<object> values, String series, int syscreator, String branchcode, Int32 zonecode, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int IsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, Int32 type, String ExpiryDate, Double version, String stationcode, String KPTN, double vat, Int32 remotezone, String RemoteBranchCode)
    {
        kplog.Info("Username: "+Username+", Password: "+Password+", series: "+series+", syscreator: "+syscreator+", branchcode: "+branchcode+", zonecode: "+zonecode+", SenderMLCardNO: "+SenderMLCardNO+", SenderFName: "+SenderFName+", SenderLName: "+SenderLName+", SenderMName: "+SenderMName+", SenderStreet: "+SenderStreet+", SenderProvinceCity: "+SenderProvinceCity+", SenderCountry: "+SenderCountry+", SenderGender: "+SenderGender+", SenderContactNo: "+SenderContactNo+", IsSMS: "+IsSMS+", SenderBirthdatE: "+SenderBirthdate+", SenderBranchID: "+SenderBranchID+", RecieverMLCardNO: "+ReceiverMLCardNO+", RecieverFName: "+ReceiverFName+", RecieverLName: "+ReceiverLName+", RecieverMName: "+ReceiverMName+", RecieverStreet: "+ReceiverStreet+", RecieverProvinceCity: "+ReceiverProvinceCity+", RecieverCountry: "+ReceiverCountry+", RecieverGender: "+ReceiverGender+", RecieverContactNo: "+ReceiverContactNo+", RecieverBirthdate: "+ReceiverBirthdate+", type: "+type+", ExpiryDate: "+ExpiryDate+", version: "+version+", stationcode: "+stationcode+", KPTN: "+KPTN+", vat: "+vat+", remotezone: "+remotezone+", RemoteBranchCode: "+RemoteBranchCode);

        try
        {
            if (values[2].ToString().Equals("0"))
            {
                kplog.Fatal("FAILED:: message: " + getRespMessage(13));
                return new SendoutResponse { respcode = 10, message = getRespMessage(13) };
            }
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new SendoutResponse { respcode = 7, message = getRespMessage(7) };
            }

            return (SendoutResponse)saveSendoutGlobal(values, series, syscreator, branchcode, zonecode, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, IsSMS, SenderBirthdate, SenderBranchID, ReceiverMLCardNO, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, type, ExpiryDate, stationcode, KPTN, vat, remotezone, RemoteBranchCode);

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: message: " + ex.Message + ", ErrorDetail: " + ex.ToString());
            return new SendoutResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }
    }


    [WebMethod(BufferResponse = false)]
    public SendoutResponse sendoutDomestic(String Username, String Password, List<object> values, String series, int syscreator, String branchcode, Int32 zonecode, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int IsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, Int32 type, String ExpiryDate, Double version, String stationcode, String KPTN, Int32 remotezone, String RemoteBranchCode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", series: " + series + ", syscreator: " + syscreator + ", branchcode: " + branchcode + ", zonecode: " + zonecode + ", SenderMLCardNO: " + SenderMLCardNO + ", SenderFName: " + SenderFName + ", SenderLName: " + SenderLName + ", SenderMName: " + SenderMName + ", SenderStreet: " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ", SenderGender: " + SenderGender + ", SenderContactNo: " + SenderContactNo + ", IsSMS: " + IsSMS + ", SenderBirthdatE: " + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", RecieverMLCardNO: " + ReceiverMLCardNO + ", RecieverFName: " + ReceiverFName + ", RecieverLName: " + ReceiverLName + ", RecieverMName: " + ReceiverMName + ", RecieverStreet: " + ReceiverStreet + ", RecieverProvinceCity: " + ReceiverProvinceCity + ", RecieverCountry: " + ReceiverCountry + ", RecieverGender: " + ReceiverGender + ", RecieverContactNo: " + ReceiverContactNo + ", RecieverBirthdate: " + ReceiverBirthdate + ", type: " + type + ", ExpiryDate: " + ExpiryDate + ", version: " + version + ", stationcode: " + stationcode + ", KPTN: " + KPTN + ", remotezone: " + remotezone + ", RemoteBranchCode: " + RemoteBranchCode);

        try
        {
            if (values[2].ToString().Equals("0"))
            {
                kplog.Fatal(getRespMessage(13));
                   kplog.Fatal("FAILED:: respcode: 10 , message :"+getRespMessage(13));
                return new SendoutResponse { respcode = 10, message = getRespMessage(13) };
            }
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new SendoutResponse { respcode = 7, message = getRespMessage(7) };
            }

            return (SendoutResponse)saveSendoutDomestic(values, series, syscreator, branchcode, zonecode, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, IsSMS, SenderBirthdate, SenderBranchID, ReceiverMLCardNO, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, type, ExpiryDate, stationcode, KPTN, remotezone, RemoteBranchCode);

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            return new SendoutResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public ValidateTransactionResponse validateTransactionGlobal(String Username, String Password, decimal Principal, string FirstName, string LastName, Double version, String stationcode)
    {
        kplog.Info("Username: "+Username+", Password: "+Password+", Principal: "+Principal+", FirstName: "+FirstName+", LastName: "+LastName+", version: "+version+", stationcode:"+stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ValidateTransactionResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ValidateTransactionResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();

                    //command.CommandText = "SELECT c.CustId FROM kpdomestic.sendout s INNER JOIN kpcustomersglobal.customers c ON s.SenderID = c.CustID  WHERE TIMEDIFF(DATE_FORMAT(NOW(), '%h:%i:%s'),DATE_FORMAT(s.TransDate, '%h:%i:%s')) <= '00:05:00' AND c.FirstName = @FirstName AND c.LastName = @LastName AND s.Principal = @amount;";
                    command.CommandText = "SELECT id FROM kpglobal.sendout WHERE TIMEDIFF(DATE_FORMAT(NOW(), '%h:%i:%s'),DATE_FORMAT(TransDate, '%h:%i:%s')) <= '00:05:00' AND SenderFName = @FirstName AND SenderLName = @LastName AND Principal = @amount;";
                    command.Parameters.AddWithValue("amount", Principal);
                    command.Parameters.AddWithValue("FirstName", FirstName);
                    command.Parameters.AddWithValue("LastName", LastName);
                    MySqlDataReader Reader = command.ExecuteReader();
                    //throw new Exception(Reader.HasRows.ToString());

                    if (Reader.Read())
                    {
                        Reader.Close();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: SUCCESS!");
                        return new ValidateTransactionResponse { respcode = 1, message = "SUCCESS" };
                    }
                    else
                    {
                        kplog.Error("FAILED:: message : Transaction validation failed.");
                        //throw new Exception(Reader["CustId"].ToString());
                        Reader.Close();
                        conn.Close();

                        return new ValidateTransactionResponse { respcode = 0, message = "FAILED" };
                    }


                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 , message : " + ex.ToString());
                conn.Close();
                return new ValidateTransactionResponse { respcode = 0, message = ex.ToString() };
            }
        }

    }

    [WebMethod]
    public ValidateTransactionResponse validateTransaction(String Username, String Password, decimal Principal, string FirstName, string LastName, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", Principal: " + Principal + ", FirstName: " + FirstName + ", LastName: " + LastName + ", version: " + version + ", stationcode:" + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ValidateTransactionResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ValidateTransactionResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            try
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();

                    //command.CommandText = "SELECT c.CustId FROM kpdomestic.sendout s INNER JOIN kpcustomersglobal.customers c ON s.SenderID = c.CustID  WHERE TIMEDIFF(DATE_FORMAT(NOW(), '%h:%i:%s'),DATE_FORMAT(s.TransDate, '%h:%i:%s')) <= '00:05:00' AND c.FirstName = @FirstName AND c.LastName = @LastName AND s.Principal = @amount;";
                    command.CommandText = "SELECT CustId FROM kpdomestic.sendout WHERE TIMEDIFF(DATE_FORMAT(NOW(), '%h:%i:%s'),DATE_FORMAT(TransDate, '%h:%i:%s')) <= '00:05:00' AND SenderFName = @FirstName AND SenderLName = @LastName AND Principal = @amount;";
                    command.Parameters.AddWithValue("amount", Principal);
                    command.Parameters.AddWithValue("FirstName", FirstName);
                    command.Parameters.AddWithValue("LastName", LastName);
                    MySqlDataReader Reader = command.ExecuteReader();
                    //throw new Exception(Reader.HasRows.ToString());

                    if (Reader.Read())
                    {
                        Reader.Close();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message : SUCCESS!");
                        return new ValidateTransactionResponse { respcode = 1, message = "SUCCESS" };
                    }
                    else
                    {
                        kplog.Error("FAILED:: message : Transaction validation failed.");
                        //throw new Exception(Reader["CustId"].ToString());
                        Reader.Close();
                        conn.Close();

                        return new ValidateTransactionResponse { respcode = 0, message = "FAILED" };
                    }


                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: message : " + ex.ToString());
                conn.Close();
                return new ValidateTransactionResponse { respcode = 0, message = ex.ToString() };
            }
        }

    }

    [WebMethod]
    public Int32 tester()
    {
        return Convert.ToInt32("000021");
    }


    [WebMethod]
    public String getFullName(String Username, String Password, String ResourceID, String BranchCode, Int32 ZoneCode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", ResourceID: " + ResourceID + ", BranchCode: " + BranchCode + ", ZoneCode: " + ZoneCode + ", version: " + version + ", stationcode:" + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            throw new Exception("Invalid credentials");
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    throw new Exception("Version does not match!");
        //}
        try
        {
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                try
                {
                    conn.Open();
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "Select fullname from kpusers.branchusers where BranchCode = @bcode and ZoneCode = @zcode and ResourceID = @rid;";
                        command.Parameters.AddWithValue("bcode", BranchCode);
                        command.Parameters.AddWithValue("zcode", ZoneCode);
                        command.Parameters.AddWithValue("rid", ResourceID);
                        using (MySqlDataReader dataReader = command.ExecuteReader())
                        {
                            if (dataReader.Read())
                            {
                                string fullname = dataReader["fullname"].ToString();
                                dataReader.Close();
                                conn.Close();

                                return fullname;
                            }
                            else
                            {
                                kplog.Error("FAILED:: message : No data found!");
                                dataReader.Close();
                                conn.Close();
                                return null;
                            }
                        }
                    }
                }
                catch (MySqlException myx)
                {
                    kplog.Fatal("FAILED:: message : " + myx.ToString());
                    conn.Close();
                    return null;
                }
            }
        }
        catch (Exception ex)
        {
         kplog.Fatal("FAILED:: message : Outer exception catched , Error Detail: "+ex.ToString());
            return null;
        }
    }

    [WebMethod]
    public SearchResponse kptnSeacrhGlobalRTS(String Username, String Password, String kptn6, String ttype)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn6: " + kptn6 + ", ttype: " + ttype);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }

        using (MySqlConnection conn = custconGlobal.getConnection())
        {
            try
            {
                conn.Open();
                string lastname = String.Empty;
                string firstname = String.Empty;
                string middlename = String.Empty;
                string name = String.Empty;
                Int32 xtype = 0;
                using (command = conn.CreateCommand())
                {
                    if (ttype == "RETURN TO SENDER")
                    {
                        String qrychkcustkptn = "Select SenderFName, SenderLName, SenderMName from " + decodeKPTNGlobal(0, kptn6) + " where KPTNNo = @kptn6";
                        command.CommandText = qrychkcustkptn;
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("kptn6", kptn6);
                        MySqlDataReader rdrchkkptn = command.ExecuteReader();
                        if (rdrchkkptn.Read())
                        {
                            lastname = rdrchkkptn["SenderLName"].ToString();
                            firstname = rdrchkkptn["SenderFName"].ToString();
                            middlename = rdrchkkptn["SenderMName"].ToString();
                        }
                        else
                        {
                            rdrchkkptn.Close();
                            conn.Close();
                            kplog.Error("FAILED:: message : KPTN not found!");
                            return new SearchResponse { respcode = 0, message = "KPTN not found" };
                        }
                        rdrchkkptn.Close();

                        name = lastname + " " + firstname;
                        //String qrycustofac = "Select firstname,lastname FROM kpcustomersglobal.ofac WHERE MATCH(firstname, lastname) AGAINST ('" + name + "')";
                        //String qrycustofac = "Select firstname,lastname FROM kpcustomersglobal.ofac WHERE (firstname=@firstname and lastname=@lastname) or (firstname=@lastname and lastname=@firstname) or (firstname='" + name + "') or (lastname='" + name + "') or (lastname=@lastname) or ((split_str(split_str(split_str(akaList,'lastName',2),'\":\"',2),'\",',1)) = @lastname)";
                        String qrycustofac = "SELECT o.firstname,o.lastname FROM kpcustomersglobal.ofac o left join kpcustomersglobal.aliasofac a on a.CustomerID = o.uid " +
                            "WHERE (o.firstname=@firstname and o.lastname=@lastname) or (o.firstname=@lastname and o.lastname=@firstname) " +
                            "or (o.firstname='" + name + "') or (o.lastname='" + name + "') or (o.lastname = @lastname) " +
                            "or (a.LastName = @lastname)";
                        command.CommandText = qrycustofac;
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("lastname", lastname);
                        command.Parameters.AddWithValue("firstname", firstname);
                        MySqlDataReader rdrcustofac = command.ExecuteReader();
                        if (rdrcustofac.Read())
                        {
                            xtype = 1;
                        }
                        rdrcustofac.Close();

                        String qrycustwatchlist = "Select LastName, FirstName FROM kpcustomersglobal.watchlist WHERE MATCH(FirstName, LastName) AGAINST ('" + name + "')";
                        command.CommandText = qrycustwatchlist;
                        command.Parameters.Clear();
                        MySqlDataReader rdrcustwatch = command.ExecuteReader();
                        if (rdrcustwatch.Read())
                        {
                            xtype = 1;
                        }
                        rdrcustwatch.Close();

                        if (xtype == 1)
                        {
                            String qrychkstat = "Select `Status` from  kpglobal.blockedtransactions where KPTN = '" + kptn6 + "' and `Status != 'DENIED'`";
                            command.CommandText = qrychkstat;
                            command.Parameters.Clear();
                            MySqlDataReader rdrstat = command.ExecuteReader();
                            if (rdrstat.Read())
                            {
                                if (rdrstat["Status"].ToString() == "APPROVE")
                                {
                                    rdrstat.Close();
                                    conn.Close();
                                    kplog.Info("SUCCESS:: message : Transaction is being approved. Return to sender can be made!");
                                    return new SearchResponse { respcode = 1, message = "Transaction is being approved. Return to sender can be made" };
                                }
                                else
                                {
                                    rdrstat.Close();
                                    conn.Close();
                                    kplog.Error("FAILED:: message : Transaction is not yet approved. No Return to sender will be made!");
                                    return new SearchResponse { respcode = 2, message = "Transaction is not yet approved. No return to sender will be made" };
                                }
                            }
                        }
                        else
                        {
                            conn.Close();
                            kplog.Error("FAILED:: message : KPTN not found as blocklisted transaction!");
                            return new SearchResponse { respcode = 0, message = "KPTN not found as blocklisted transaction" };
                        }
                    }
                    else
                    {
                        conn.Close();
                        kplog.Info("SUCCESS:: message : Transaction is not return to sender");
                        return new SearchResponse { respcode = 1, message = "Transaction is not return to sender" };
                    }

                    conn.Close();
                    kplog.Error("FAILED:: message: " + getRespMessage(0));
                    return new SearchResponse { respcode = 0, message = getRespMessage(0) };
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                conn.Close();
                if (ex.Message.Equals("4"))
                {
                    kplog.Fatal("FAILED:: respcode: 4 , message : " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                return new SearchResponse { respcode = 0, message = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }

    [WebMethod(BufferResponse = false, Description = "Method for searching Global Transactions")]
    public SearchResponse kptnSearchGlobalMobile(String Username, String Password, String kptn, String kptn6, Decimal amount, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn6: " + kptn6 + ", kptn: " + kptn + ", amount: " + amount + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }

        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                conn.Open();

                using (command = conn.CreateCommand())
                {
                    List<object> a = new List<object>();

                    SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();

                    String query = "SELECT Purpose, ZoneCode, BranchCode, IsClaimed, IsCancelled , RemoteBranch, RemoteOperatorID,IsRemote, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, ORNo, Charge, RemoteZoneCode, vat, preferredcurrency, exchangerate, amountpo,paymenttype,bankname,cardcheckno,cardcheckexpdate,TransType,OtherCharge FROM " + decodeKPTNGlobalMobile(0, kptn6) + " WHERE KPTNNo = @kptn6;";

                    command.CommandText = query;
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("kptn6", kptn6);
                    using (MySqlDataReader dataReader = command.ExecuteReader())
                    {
                        if (dataReader.HasRows)
                        {
                            dataReader.Read();

                            string sFName = dataReader["SenderFname"].ToString();
                            string sLName = dataReader["SenderLname"].ToString();
                            string sMName = dataReader["SenderMName"].ToString();
                            string sSt = dataReader["SenderStreet"].ToString();
                            string sPCity = dataReader["SenderProvinceCity"].ToString();
                            string sCtry = dataReader["SenderCountry"].ToString();
                            string sG = dataReader["SenderGender"].ToString();
                            string sCNo = dataReader["SenderContactNo"].ToString();
                            Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                            string sBID = dataReader["SenderBranchID"].ToString();
                            string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                            string SenderName = dataReader["SenderName"].ToString();

                            string rFName = dataReader["ReceiverFname"].ToString();
                            string rLName = dataReader["ReceiverLname"].ToString();
                            string rMName = dataReader["ReceiverMName"].ToString();
                            string rSt = dataReader["ReceiverStreet"].ToString();
                            string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                            string rCtry = dataReader["ReceiverCountry"].ToString();
                            string rG = dataReader["ReceiverGender"].ToString();
                            string rCNo = dataReader["ReceiverContactNo"].ToString();
                            string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                            string ReceiverName = dataReader["ReceiverName"].ToString();

                            string SendoutControlNo = dataReader["ControlNo"].ToString();
                            string KPTNNo = dataReader["KPTNNo"].ToString();
                            string OperatorID = dataReader["OperatorID"].ToString();
                            Boolean IsPassword = Convert.ToBoolean(dataReader["IsPassword"]);
                            string TransPassword = dataReader["TransPassword"].ToString();
                            DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"]);
                            string Currency = dataReader["Currency"].ToString();
                            Decimal Principal = (Decimal)dataReader["Principal"];
                            Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string Relation = dataReader["Relation"].ToString();
                            string Message = dataReader["Message"].ToString();
                            string StationID = dataReader["StationID"].ToString();
                            string SourceOfFund = dataReader["Source"].ToString();
                            string IDType = dataReader["IDType"].ToString();
                            string IDNo = dataReader["IDNo"].ToString();
                            string ExpiryDate = dataReader["ExpiryDate"].ToString();
                            string ORNo = dataReader["ORNo"].ToString();
                            string RemoteBranch = dataReader["RemoteBranch"].ToString();
                            string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                            Boolean isSameAmount = false;
                            Boolean IsRemote = Convert.ToBoolean(dataReader["IsRemote"]);
                            Boolean x = Convert.ToBoolean(dataReader["IsCancelled"]);
                            Boolean IsClaimed = Convert.ToBoolean(dataReader["IsClaimed"]);
                            string bcode = dataReader["BranchCode"].ToString();
                            Decimal Charge = Convert.ToDecimal(dataReader["Charge"]);
                            Int32 zcode = Convert.ToInt32(dataReader["ZoneCode"]);
                            string purpose = dataReader["Purpose"].ToString();
                            Int32? remoteZone = dataReader["RemoteZoneCode"] == DBNull.Value ? (Int32?)null : Convert.ToInt32(dataReader["RemoteZoneCode"]);
                            double? vat = dataReader["vat"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["vat"]);
                            string precurrency = dataReader["preferredcurrency"].ToString();
                            double? xchangerate = dataReader["exchangerate"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["exchangerate"]);
                            double? amtpo = dataReader["amountpo"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["amountpo"]);
                            String paymenttype = dataReader["paymenttype"].ToString();
                            String bankname = dataReader["bankname"].ToString();
                            String cardcheckno = dataReader["cardcheckno"].ToString();
                            String cardexpdate = (dataReader["cardcheckexpdate"].ToString() == String.Empty || dataReader["cardcheckexpdate"].ToString().StartsWith("0")) ? null : DateTime.Parse(dataReader["cardcheckexpdate"].ToString()).ToString("yyyy-MM-dd");
                            String trxntype = dataReader["TransType"].ToString();
                            Decimal othchg = Convert.ToDecimal(dataReader["OtherCharge"]);

                            if (amtpo == Convert.ToDouble(amount))
                            {
                                isSameAmount = true;
                            }


                            if (x)
                            {
                                kplog.Error("FAILED:: respcode: 8, message: " + getRespMessage(8) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 8, message = getRespMessage(8), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            if (IsClaimed)
                            {
                                kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(3) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 3, message = getRespMessage(3), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            dataReader.Close();

                            //Start of Checking KPTN BlockTransaction

                            //String qrychkblockofac = "Select KPTN FROM Mobexglobal.blockedtransactionsmobile where `Status` NOT IN ('APPROVE','APPROVED','DENY','DENIED') and ReasonForHold = 'OFAC' and KPTN = @kptnblock";
                            //command.CommandText = qrychkblockofac;
                            //command.Parameters.Clear();
                            //command.Parameters.AddWithValue("kptnblock", kptn6);
                            //MySqlDataReader rdrchkblockofac = command.ExecuteReader();
                            //if (rdrchkblockofac.Read())
                            //{
                            //    rdrchkblockofac.Close();
                            //    conn.Close();
                            //    return new SearchResponse { respcode = 0, message = "OFAC Transaction being block should be approved or denied first before cancellation will be made" };
                            //}
                            //rdrchkblockofac.Close();

                            //End of Checking KPTN BlockTransaction

                            Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);
                            conn.Close();
                            SenderInfo si = new SenderInfo
                            {
                                FirstName = sFName,
                                LastName = sLName,
                                MiddleName = sMName,
                                SenderName = SenderName,
                                Street = sSt,
                                ProvinceCity = sPCity,
                                Country = sCtry,
                                Gender = sG,
                                ContactNo = sCNo,
                                IsSMS = sIsSM,
                                BranchID = sBID,
                                SenderMLCardNo = sMLCardNo,
                                Birthdate = sBdate
                            };

                            ReceiverInfo ri = new ReceiverInfo
                            {
                                FirstName = rFName,
                                LastName = rLName,
                                MiddleName = rMName,
                                ReceiverName = ReceiverName,
                                Street = rSt,
                                ProvinceCity = rPCity,
                                Country = rCtry,
                                Gender = rG,
                                ContactNo = rCNo,
                                BirthDate = rBdate,
                            };

                            SendoutInfo soi = new SendoutInfo
                            {
                                SendoutControlNo = SendoutControlNo,
                                KPTNNo = KPTNNo,
                                OperatorID = OperatorID,
                                IsPassword = IsPassword,
                                TransPassword = TransPassword,
                                syscreated = syscreated,
                                Currency = Currency,
                                Principal = Principal,
                                SenderIsSMS = SenderIsSMS,
                                Relation = Relation,
                                Message = Message,
                                StationID = StationID,
                                SourceOfFund = SourceOfFund,
                                IDNo = IDNo,
                                IDType = IDType,
                                ExpiryDate = ExpiryDate,
                                DormantCharge = DormantCharge,
                                ORNo = ORNo,
                                isSameAmount = isSameAmount,
                                IsRemote = IsRemote,
                                RemoteBranch = RemoteBranch,
                                RemoteOperatorID = RemoteOperatorID,
                                BranchCode = bcode,
                                Charge = Charge,
                                ZoneCode = zcode,
                                Purpose = purpose,
                                RemoteZone = remoteZone,
                                vat = vat,
                                preferredcurrency = precurrency,
                                exchangerate = xchangerate,
                                amountpo = amtpo,
                                paytype = paymenttype,
                                bankname = bankname,
                                cardcheck = cardcheckno,
                                cardexp = cardexpdate,
                                transtp = trxntype,
                                OtherCharge = othchg
                            };
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                            return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                        }
                        else
                        {
                            kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                            dataReader.Close();
                            conn.Close();
                            return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                        }
                    }
                }
            }
            catch (Exception ex)
            {
               
                conn.Close();
                if (ex.Message.Equals("4"))
                {
                    kplog.Fatal("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
                kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(4) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                return new SearchResponse { respcode = 0, message = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }

    [WebMethod(BufferResponse = false, Description = "Method for searching Global Transactions")]
    public SearchResponse kptnSearchGlobalKioskGlobal(String Username, String Password, String kptn, String kptn6, Decimal amount, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn6: " + kptn6 + ", kptn: " + kptn + ", amount: " + amount + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }

        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                conn.Open();

                using (command = conn.CreateCommand())
                {
                    List<object> a = new List<object>();

                    SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();

                    String query = "SELECT Purpose, ZoneCode, BranchCode, IsClaimed, IsCancelled , RemoteBranch, RemoteOperatorID,IsRemote, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, ORNo, Charge, RemoteZoneCode, vat, preferredcurrency, exchangerate, amountpo,paymenttype,bankname,cardcheckno,cardcheckexpdate,TransType,OtherCharge FROM " + decodeKPTNGlobalKioskGlobal(0, kptn6) + " WHERE KPTNNo = @kptn6;";

                    command.CommandText = query;
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("kptn6", kptn6);
                    using (MySqlDataReader dataReader = command.ExecuteReader())
                    {
                        if (dataReader.HasRows)
                        {
                            dataReader.Read();

                            string sFName = dataReader["SenderFname"].ToString();
                            string sLName = dataReader["SenderLname"].ToString();
                            string sMName = dataReader["SenderMName"].ToString();
                            string sSt = dataReader["SenderStreet"].ToString();
                            string sPCity = dataReader["SenderProvinceCity"].ToString();
                            string sCtry = dataReader["SenderCountry"].ToString();
                            string sG = dataReader["SenderGender"].ToString();
                            string sCNo = dataReader["SenderContactNo"].ToString();
                            Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                            string sBID = dataReader["SenderBranchID"].ToString();
                            string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                            string SenderName = dataReader["SenderName"].ToString();

                            string rFName = dataReader["ReceiverFname"].ToString();
                            string rLName = dataReader["ReceiverLname"].ToString();
                            string rMName = dataReader["ReceiverMName"].ToString();
                            string rSt = dataReader["ReceiverStreet"].ToString();
                            string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                            string rCtry = dataReader["ReceiverCountry"].ToString();
                            string rG = dataReader["ReceiverGender"].ToString();
                            string rCNo = dataReader["ReceiverContactNo"].ToString();
                            string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                            string ReceiverName = dataReader["ReceiverName"].ToString();

                            string SendoutControlNo = dataReader["ControlNo"].ToString();
                            string KPTNNo = dataReader["KPTNNo"].ToString();
                            string OperatorID = dataReader["OperatorID"].ToString();
                            Boolean IsPassword = Convert.ToBoolean(dataReader["IsPassword"]);
                            string TransPassword = dataReader["TransPassword"].ToString();
                            DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"]);
                            string Currency = dataReader["Currency"].ToString();
                            Decimal Principal = (Decimal)dataReader["Principal"];
                            Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string Relation = dataReader["Relation"].ToString();
                            string Message = dataReader["Message"].ToString();
                            string StationID = dataReader["StationID"].ToString();
                            string SourceOfFund = dataReader["Source"].ToString();
                            string IDType = dataReader["IDType"].ToString();
                            string IDNo = dataReader["IDNo"].ToString();
                            string ExpiryDate = dataReader["ExpiryDate"].ToString();
                            string ORNo = dataReader["ORNo"].ToString();
                            string RemoteBranch = dataReader["RemoteBranch"].ToString();
                            string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                            Boolean isSameAmount = false;
                            Boolean IsRemote = Convert.ToBoolean(dataReader["IsRemote"]);
                            Boolean x = Convert.ToBoolean(dataReader["IsCancelled"]);
                            Boolean IsClaimed = Convert.ToBoolean(dataReader["IsClaimed"]);
                            string bcode = dataReader["BranchCode"].ToString();
                            Decimal Charge = Convert.ToDecimal(dataReader["Charge"]);
                            Int32 zcode = Convert.ToInt32(dataReader["ZoneCode"]);
                            string purpose = dataReader["Purpose"].ToString();
                            Int32? remoteZone = dataReader["RemoteZoneCode"] == DBNull.Value ? (Int32?)null : Convert.ToInt32(dataReader["RemoteZoneCode"]);
                            double? vat = dataReader["vat"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["vat"]);
                            string precurrency = dataReader["preferredcurrency"].ToString();
                            double? xchangerate = dataReader["exchangerate"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["exchangerate"]);
                            double? amtpo = dataReader["amountpo"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["amountpo"]);
                            String paymenttype = dataReader["paymenttype"].ToString();
                            String bankname = dataReader["bankname"].ToString();
                            String cardcheckno = dataReader["cardcheckno"].ToString();
                            String cardexpdate = (dataReader["cardcheckexpdate"].ToString() == String.Empty || dataReader["cardcheckexpdate"].ToString().StartsWith("0")) ? null : DateTime.Parse(dataReader["cardcheckexpdate"].ToString()).ToString("yyyy-MM-dd");
                            String trxntype = dataReader["TransType"].ToString();
                            Decimal othchg = Convert.ToDecimal(dataReader["OtherCharge"]);

                            if (amtpo == Convert.ToDouble(amount))
                            {
                                isSameAmount = true;
                            }


                            if (x)
                            {
                                kplog.Error("FAILED:: respcode: 8, message: " + getRespMessage(8) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 8, message = getRespMessage(8), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            if (IsClaimed)
                            {
                                kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(3) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 3, message = getRespMessage(3), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            dataReader.Close();

                            //Start of Checking KPTN BlockTransaction

                            //String qrychkblockofac = "Select KPTN FROM Mobexglobal.blockedtransactionsmobile where `Status` NOT IN ('APPROVE','APPROVED','DENY','DENIED') and ReasonForHold = 'OFAC' and KPTN = @kptnblock";
                            //command.CommandText = qrychkblockofac;
                            //command.Parameters.Clear();
                            //command.Parameters.AddWithValue("kptnblock", kptn6);
                            //MySqlDataReader rdrchkblockofac = command.ExecuteReader();
                            //if (rdrchkblockofac.Read())
                            //{
                            //    rdrchkblockofac.Close();
                            //    conn.Close();
                            //    return new SearchResponse { respcode = 0, message = "OFAC Transaction being block should be approved or denied first before cancellation will be made" };
                            //}
                            //rdrchkblockofac.Close();

                            //End of Checking KPTN BlockTransaction

                            Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);
                            conn.Close();
                            SenderInfo si = new SenderInfo
                            {
                                FirstName = sFName,
                                LastName = sLName,
                                MiddleName = sMName,
                                SenderName = SenderName,
                                Street = sSt,
                                ProvinceCity = sPCity,
                                Country = sCtry,
                                Gender = sG,
                                ContactNo = sCNo,
                                IsSMS = sIsSM,
                                BranchID = sBID,
                                SenderMLCardNo = sMLCardNo,
                                Birthdate = sBdate
                            };

                            ReceiverInfo ri = new ReceiverInfo
                            {
                                FirstName = rFName,
                                LastName = rLName,
                                MiddleName = rMName,
                                ReceiverName = ReceiverName,
                                Street = rSt,
                                ProvinceCity = rPCity,
                                Country = rCtry,
                                Gender = rG,
                                ContactNo = rCNo,
                                BirthDate = rBdate,
                            };

                            SendoutInfo soi = new SendoutInfo
                            {
                                SendoutControlNo = SendoutControlNo,
                                KPTNNo = KPTNNo,
                                OperatorID = OperatorID,
                                IsPassword = IsPassword,
                                TransPassword = TransPassword,
                                syscreated = syscreated,
                                Currency = Currency,
                                Principal = Principal,
                                SenderIsSMS = SenderIsSMS,
                                Relation = Relation,
                                Message = Message,
                                StationID = StationID,
                                SourceOfFund = SourceOfFund,
                                IDNo = IDNo,
                                IDType = IDType,
                                ExpiryDate = ExpiryDate,
                                DormantCharge = DormantCharge,
                                ORNo = ORNo,
                                isSameAmount = isSameAmount,
                                IsRemote = IsRemote,
                                RemoteBranch = RemoteBranch,
                                RemoteOperatorID = RemoteOperatorID,
                                BranchCode = bcode,
                                Charge = Charge,
                                ZoneCode = zcode,
                                Purpose = purpose,
                                RemoteZone = remoteZone,
                                vat = vat,
                                preferredcurrency = precurrency,
                                exchangerate = xchangerate,
                                amountpo = amtpo,
                                paytype = paymenttype,
                                bankname = bankname,
                                cardcheck = cardcheckno,
                                cardexp = cardexpdate,
                                transtp = trxntype,
                                OtherCharge = othchg
                            };
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", RecieverInfo : " + ri + ", SendoutInfo: " + soi);
                            return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                        }
                        else
                        {
                            kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                            dataReader.Close();
                            conn.Close();
                            return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                conn.Close();
                if (ex.Message.Equals("4"))
                {
                    kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
                return new SearchResponse { respcode = 0, message = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }

    [WebMethod(BufferResponse = false, Description = "Method for searching Global Transactions")]
    public SearchResponse kptnSearchGlobal(String Username, String Password, String kptn, String kptn6, Decimal amount, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn6: " + kptn6 + ", kptn: " + kptn + ", amount: " + amount + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }

        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            //DateTime TransDate;
            try
            {
                conn.Open();

                using (command = conn.CreateCommand())
                {
                    List<object> a = new List<object>();

                    SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();

                    //String qrychkblockofac = "Select KPTN FROM kpglobal.blockedtransactions where `Status` NOT IN ('APPROVE','APPROVED','DENY','DENIED') and ReasonForHold = 'OFAC' and KPTN = @kptnblock";
                    //command.CommandText = qrychkblockofac;
                    //command.Parameters.Clear();
                    //command.Parameters.AddWithValue("kptnblock", kptn6);
                    //MySqlDataReader rdrchkblockofac = command.ExecuteReader();
                    //if (rdrchkblockofac.Read())
                    //{
                    //    rdrchkblockofac.Close();
                    //    conn.Close();
                    //    return new SearchResponse { respcode = 0, message = "OFAC Transaction being block should be approved or denied first before cancellation will be made" };
                    //}
                    //rdrchkblockofac.Close();

                    String query = "SELECT Purpose, ZoneCode, BranchCode, IsClaimed, IsCancelled , RemoteBranch, RemoteOperatorID,IsRemote, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, ORNo, Charge, RemoteZoneCode, vat, preferredcurrency, exchangerate, amountpo,paymenttype,bankname,cardcheckno,cardcheckexpdate,TransType,OtherCharge FROM " + decodeKPTNGlobal(0, kptn6) + " WHERE KPTNNo = @kptn6;";

                    command.CommandText = query;
                    //command.Parameters.AddWithValue("kptn", kptn);
                    command.Parameters.AddWithValue("kptn6", kptn6);
                    using (MySqlDataReader dataReader = command.ExecuteReader())
                    {
                        if (dataReader.HasRows)
                        {
                            dataReader.Read();

                            string sFName = dataReader["SenderFname"].ToString();
                            string sLName = dataReader["SenderLname"].ToString();
                            string sMName = dataReader["SenderMName"].ToString();
                            string sSt = dataReader["SenderStreet"].ToString();
                            string sPCity = dataReader["SenderProvinceCity"].ToString();
                            string sCtry = dataReader["SenderCountry"].ToString();
                            string sG = dataReader["SenderGender"].ToString();
                            string sCNo = dataReader["SenderContactNo"].ToString();
                            Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                            string sBID = dataReader["SenderBranchID"].ToString();
                            //string sCustID = dataReader["CustID"].ToString();
                            string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                            string SenderName = dataReader["SenderName"].ToString();

                            string rFName = dataReader["ReceiverFname"].ToString();
                            string rLName = dataReader["ReceiverLname"].ToString();
                            string rMName = dataReader["ReceiverMName"].ToString();
                            string rSt = dataReader["ReceiverStreet"].ToString();
                            string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                            string rCtry = dataReader["ReceiverCountry"].ToString();
                            string rG = dataReader["ReceiverGender"].ToString();
                            string rCNo = dataReader["ReceiverContactNo"].ToString();
                            string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                            //string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                            string ReceiverName = dataReader["ReceiverName"].ToString();

                            string SendoutControlNo = dataReader["ControlNo"].ToString();
                            string KPTNNo = dataReader["KPTNNo"].ToString();
                            //string kptn4 = dataReader["kptn4"].ToString();
                            string OperatorID = dataReader["OperatorID"].ToString();
                            Boolean IsPassword = Convert.ToBoolean(dataReader["IsPassword"]);
                            string TransPassword = dataReader["TransPassword"].ToString();
                            DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"]);
                            string Currency = dataReader["Currency"].ToString();
                            Decimal Principal = (Decimal)dataReader["Principal"];
                            //string SenderID = dataReader["CustID"].ToString();
                            Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string Relation = dataReader["Relation"].ToString();
                            string Message = dataReader["Message"].ToString();
                            string StationID = dataReader["StationID"].ToString();
                            string SourceOfFund = dataReader["Source"].ToString();
                            string IDType = dataReader["IDType"].ToString();
                            string IDNo = dataReader["IDNo"].ToString();
                            string ExpiryDate = dataReader["ExpiryDate"].ToString();
                            string ORNo = dataReader["ORNo"].ToString();
                            string RemoteBranch = dataReader["RemoteBranch"].ToString();
                            string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                            Boolean isSameAmount = false;
                            Boolean IsRemote = Convert.ToBoolean(dataReader["IsRemote"]);
                            Boolean x = Convert.ToBoolean(dataReader["IsCancelled"]);
                            Boolean IsClaimed = Convert.ToBoolean(dataReader["IsClaimed"]);
                            string bcode = dataReader["BranchCode"].ToString();
                            Decimal Charge = Convert.ToDecimal(dataReader["Charge"]);
                            Int32 zcode = Convert.ToInt32(dataReader["ZoneCode"]);
                            string purpose = dataReader["Purpose"].ToString();
                            Int32? remoteZone = dataReader["RemoteZoneCode"] == DBNull.Value ? (Int32?)null : Convert.ToInt32(dataReader["RemoteZoneCode"]);
                            double? vat = dataReader["vat"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["vat"]);
                            string precurrency = dataReader["preferredcurrency"].ToString();
                            double? xchangerate = dataReader["exchangerate"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["exchangerate"]);
                            double? amtpo = dataReader["amountpo"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["amountpo"]);
                            String paymenttype = dataReader["paymenttype"].ToString();
                            String bankname = dataReader["bankname"].ToString();
                            String cardcheckno = dataReader["cardcheckno"].ToString();
                            String cardexpdate = (dataReader["cardcheckexpdate"].ToString() == String.Empty || dataReader["cardcheckexpdate"].ToString().StartsWith("0")) ? null : DateTime.Parse(dataReader["cardcheckexpdate"].ToString()).ToString("yyyy-MM-dd");
                            String trxntype = dataReader["TransType"].ToString();
                            Decimal othchg = Convert.ToDecimal(dataReader["OtherCharge"]);

                            if (amtpo == Convert.ToDouble(amount))
                            {
                                isSameAmount = true;
                            }


                            if (x)
                            {
                                kplog.Error("FAILED:: respcode: 8, message: " + getRespMessage(8) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 8, message = getRespMessage(8), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            if (IsClaimed)
                            {
                                kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(3) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 3, message = getRespMessage(3), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            dataReader.Close();

                            //Start of Checking KPTN BlockTransaction

                            String qrychkblockofac = "Select KPTN FROM kpglobal.blockedtransactions where `Status` NOT IN ('APPROVE','APPROVED','DENY','DENIED') and ReasonForHold = 'OFAC' and KPTN = @kptnblock";
                            command.CommandText = qrychkblockofac;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("kptnblock", kptn6);
                            MySqlDataReader rdrchkblockofac = command.ExecuteReader();
                            if (rdrchkblockofac.Read())
                            {
                                rdrchkblockofac.Close();
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 0 , message: OFAC Transaction being block should be approved or denied first before cancellation will be made!");
                                return new SearchResponse { respcode = 0, message = "OFAC Transaction being block should be approved or denied first before cancellation will be made" };
                            }
                            rdrchkblockofac.Close();

                            //End of Checking KPTN BlockTransaction

                            Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);
                            conn.Close();
                            SenderInfo si = new SenderInfo
                            {
                                FirstName = sFName,
                                LastName = sLName,
                                MiddleName = sMName,
                                SenderName = SenderName,
                                Street = sSt,
                                ProvinceCity = sPCity,
                                Country = sCtry,
                                Gender = sG,
                                ContactNo = sCNo,
                                IsSMS = sIsSM,
                                BranchID = sBID,
                                //CustID = sCustID,
                                SenderMLCardNo = sMLCardNo,
                                Birthdate = sBdate
                            };

                            ReceiverInfo ri = new ReceiverInfo
                            {
                                FirstName = rFName,
                                LastName = rLName,
                                MiddleName = rMName,
                                ReceiverName = ReceiverName,
                                Street = rSt,
                                ProvinceCity = rPCity,
                                Country = rCtry,
                                Gender = rG,
                                ContactNo = rCNo,
                                BirthDate = rBdate,
                                //MLCardNo = rMLCardNo
                            };

                            SendoutInfo soi = new SendoutInfo
                            {
                                SendoutControlNo = SendoutControlNo,
                                KPTNNo = KPTNNo,
                                OperatorID = OperatorID,
                                IsPassword = IsPassword,
                                TransPassword = TransPassword,
                                syscreated = syscreated,
                                Currency = Currency,
                                Principal = Principal,
                                //SenderID = SenderID,
                                SenderIsSMS = SenderIsSMS,
                                Relation = Relation,
                                Message = Message,
                                StationID = StationID,
                                SourceOfFund = SourceOfFund,
                                //kptn4 = kptn4,
                                IDNo = IDNo,
                                IDType = IDType,
                                ExpiryDate = ExpiryDate,
                                DormantCharge = DormantCharge,
                                ORNo = ORNo,
                                isSameAmount = isSameAmount,
                                IsRemote = IsRemote,
                                RemoteBranch = RemoteBranch,
                                RemoteOperatorID = RemoteOperatorID,
                                BranchCode = bcode,
                                Charge = Charge,
                                ZoneCode = zcode,
                                Purpose = purpose,
                                RemoteZone = remoteZone,
                                vat = vat,
                                preferredcurrency = precurrency,
                                exchangerate = xchangerate,
                                amountpo = amtpo,
                                paytype = paymenttype,
                                bankname = bankname,
                                cardcheck = cardcheckno,
                                cardexp = cardexpdate,
                                transtp = trxntype,
                                OtherCharge = othchg
                            };
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo : " + soi);
                            return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                        }
                        else
                        {
                            kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                            dataReader.Close();
                            conn.Close();
                            return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                conn.Close();
                if (ex.Message.Equals("4"))
                {
                    kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null); ;
                    return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
                return new SearchResponse { respcode = 0, message = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }

    [WebMethod(BufferResponse = false, Description = "Method for searching Domestic Transactions")]
    public SearchResponse kptnSearchDomestic(String Username, String Password, String kptn, String kptn6, Decimal amount, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn6: " + kptn6 + ", kptn: " + kptn + ", amount: " + amount + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }

        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new SearchResponse { respcode = 10, message = getRespMessage(10) };
        //}

        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            //DateTime TransDate;
            try
            {
                conn.Open();

                using (command = conn.CreateCommand())
                {
                    List<object> a = new List<object>();

                    SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();
                    // AND IsClaimed = 0 AND IsCancelled = 0
                    //String query = "SELECT SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, kptn4, IDType, IDNo, ExpiryDate, SenderName, ReceiverName FROM " + generateTableName(2) + " as t INNER JOIN ON " + generateTableName(0) + " s ON t.KPTN6 = s.KPTNNo and t.MLKP4TN = s.kptn4 WHERE (MLKP4TN = @kptn OR MLKP4TN = @kptn) and IsClaimed = 0;";
                    if ((decodeKPTNDomestic(0, kptn6)) != "4")
                    {
                        String query = "SELECT Purpose, ZoneCode, BranchCode, IsClaimed, IsCancelled , RemoteBranch, RemoteOperatorID,IsRemote, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, ORNo, Charge, RemoteZoneCode FROM " + decodeKPTNDomestic(0, kptn6) + " WHERE KPTNNo = @kptn6;";
                        command.CommandText = query;
                        command.Parameters.AddWithValue("kptn6", kptn6);
                    }
                    else
                    {
                        conn.Close();
                        kplog.Error("FAILED:: respcode: 0 , message : Invalid KPTN!");
                        return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                    }
                    //throw new Exception(decodeKPTN(0, kptn6));


                    //command.CommandText = query;
                    //command.Parameters.AddWithValue("kptn", kptn);
                    //command.Parameters.AddWithValue("kptn6", kptn6);
                    using (MySqlDataReader dataReader = command.ExecuteReader())
                    {
                        if (dataReader.HasRows)
                        {
                            dataReader.Read();

                            string sFName = dataReader["SenderFname"].ToString();
                            string sLName = dataReader["SenderLname"].ToString();
                            string sMName = dataReader["SenderMName"].ToString();
                            string sSt = dataReader["SenderStreet"].ToString();
                            string sPCity = dataReader["SenderProvinceCity"].ToString();
                            string sCtry = dataReader["SenderCountry"].ToString();
                            string sG = dataReader["SenderGender"].ToString();
                            string sCNo = dataReader["SenderContactNo"].ToString();
                            Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                            string sBID = dataReader["SenderBranchID"].ToString();
                            string sCustID = dataReader["CustID"].ToString();
                            string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                            string SenderName = dataReader["SenderName"].ToString();

                            string rFName = dataReader["ReceiverFname"].ToString();
                            string rLName = dataReader["ReceiverLname"].ToString();
                            string rMName = dataReader["ReceiverMName"].ToString();
                            string rSt = dataReader["ReceiverStreet"].ToString();
                            string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                            string rCtry = dataReader["ReceiverCountry"].ToString();
                            string rG = dataReader["ReceiverGender"].ToString();
                            string rCNo = dataReader["ReceiverContactNo"].ToString();
                            string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                            string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                            string ReceiverName = dataReader["ReceiverName"].ToString();

                            string SendoutControlNo = dataReader["ControlNo"].ToString();
                            string KPTNNo = dataReader["KPTNNo"].ToString();
                            //string kptn4 = dataReader["kptn4"].ToString();
                            string OperatorID = dataReader["OperatorID"].ToString();
                            bool IsPassword = (bool)dataReader["IsPassword"];
                            string TransPassword = dataReader["TransPassword"].ToString();
                            DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"]);
                            string Currency = dataReader["Currency"].ToString();
                            Decimal Principal = (Decimal)dataReader["Principal"];
                            string SenderID = dataReader["CustID"].ToString();
                            Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                            string Relation = dataReader["Relation"].ToString();
                            string Message = dataReader["Message"].ToString();
                            string StationID = dataReader["StationID"].ToString();
                            string SourceOfFund = dataReader["Source"].ToString();
                            string IDType = dataReader["IDType"].ToString();
                            string IDNo = dataReader["IDNo"].ToString();
                            string ExpiryDate = dataReader["ExpiryDate"].ToString();
                            string ORNo = dataReader["ORNo"].ToString();
                            string RemoteBranch = dataReader["RemoteBranch"].ToString();
                            string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                            Boolean isSameAmount = false;
                            bool IsRemote = (bool)dataReader["IsRemote"];
                            bool x = (bool)dataReader["IsCancelled"];
                            bool IsClaimed = (bool)dataReader["IsClaimed"];
                            string bcode = dataReader["BranchCode"].ToString();
                            Decimal Charge = Convert.ToDecimal(dataReader["Charge"]);
                            Int32 zcode = Convert.ToInt32(dataReader["ZoneCode"]);
                            string purpose = dataReader["Purpose"].ToString();
                            Int32? remoteZone = dataReader["RemoteZoneCode"] == DBNull.Value ? (Int32?)null : Convert.ToInt32(dataReader["RemoteZoneCode"]);
                            //double? vat = dataReader["vat"] == DBNull.Value ? 0.0 : Convert.ToDouble(dataReader["vat"]);

                            if (Principal == amount)
                            {
                                isSameAmount = true;
                            }


                            if (x)
                            {
                                kplog.Error(getRespMessage(8));
                                dataReader.Close();
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 8, message: " + getRespMessage(8) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                return new SearchResponse { respcode = 8, message = getRespMessage(8), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            if (IsClaimed)
                            {
                                kplog.Error(getRespMessage(3));
                                dataReader.Close();
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(3) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                return new SearchResponse { respcode = 3, message = getRespMessage(3), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                            }
                            dataReader.Close();
                            Decimal DormantCharge = CalculateDormantChargeDomestic(syscreated);
                            conn.Close();
                            SenderInfo si = new SenderInfo
                            {
                                FirstName = sFName,
                                LastName = sLName,
                                MiddleName = sMName,
                                SenderName = SenderName,
                                Street = sSt,
                                ProvinceCity = sPCity,
                                Country = sCtry,
                                Gender = sG,
                                ContactNo = sCNo,
                                IsSMS = sIsSM,
                                BranchID = sBID,
                                CustID = sCustID,
                                SenderMLCardNo = sMLCardNo,
                                Birthdate = sBdate
                            };

                            ReceiverInfo ri = new ReceiverInfo
                            {
                                FirstName = rFName,
                                LastName = rLName,
                                MiddleName = rMName,
                                ReceiverName = ReceiverName,
                                Street = rSt,
                                ProvinceCity = rPCity,
                                Country = rCtry,
                                Gender = rG,
                                ContactNo = rCNo,
                                BirthDate = rBdate,
                                MLCardNo = rMLCardNo
                            };

                            SendoutInfo soi = new SendoutInfo
                            {
                                SendoutControlNo = SendoutControlNo,
                                KPTNNo = KPTNNo,
                                OperatorID = OperatorID,
                                IsPassword = IsPassword,
                                TransPassword = TransPassword,
                                syscreated = syscreated,
                                Currency = Currency,
                                Principal = Principal,
                                SenderID = SenderID,
                                SenderIsSMS = SenderIsSMS,
                                Relation = Relation,
                                Message = Message,
                                StationID = StationID,
                                SourceOfFund = SourceOfFund,
                                //kptn4 = kptn4,
                                IDNo = IDNo,
                                IDType = IDType,
                                ExpiryDate = ExpiryDate,
                                DormantCharge = DormantCharge,
                                ORNo = ORNo,
                                isSameAmount = isSameAmount,
                                IsRemote = IsRemote,
                                RemoteBranch = RemoteBranch,
                                RemoteOperatorID = RemoteOperatorID,
                                BranchCode = bcode,
                                Charge = Charge,
                                ZoneCode = zcode,
                                Purpose = purpose,
                                RemoteZone = remoteZone,
                                vat = 0.0

                            };
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                            return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                        }
                        else
                        {
                            kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                            dataReader.Close();
                            conn.Close();
                            return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                conn.Close();
                if (ex.Message.Equals("4"))
                {
                    kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
                return new SearchResponse { respcode = 0, message = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }


    [WebMethod(BufferResponse = false)]
    public SearchResponse kptnSendoutCancelSearch(String Username, String Password, String kptn, String BranchCode, int ZoneCode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", BranchCode: " + BranchCode + ", kptn: " + kptn + ", ZoneCode: " + ZoneCode + ", version: " + version + ", stationcode: " + stationcode);

        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new SearchResponse { respcode = 10, message = getRespMessage(10) };
        //}
        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            DateTime TransDate;
            try
            {
                conn.Open();
                using (command = conn.CreateCommand())
                {
                    String query = "Select TransDate, IsClaimed, IsCancelled from " + generateTableNameGlobal(2, null) + " WHERE (KPTN6 = @kptn OR MLKP4TN = @kptn) AND IsClaimed = 0 AND IsCancelled = 0;";
                    command.CommandText = query;

                    command.Parameters.AddWithValue("kptn", kptn);
                    MySqlDataReader dataReader = command.ExecuteReader();
                    if (dataReader.Read())
                    {

                        //throw new Exception(dataReader["TransDate"].GetType().ToString());
                        TransDate = Convert.ToDateTime(dataReader["TransDate"]);
                        dataReader.Close();
                    }
                    else
                    {
                        dataReader.Close();

                        using (command = conn.CreateCommand())
                        {
                            String querylocal = "Select TransDate, IsClaimed, IsCancelled from " + generateTableNameGlobal(2, null) + " WHERE (KPTN6 = @kptn OR MLKP4TN = @kptn) ORDER BY TransDate DESC LIMIT 1;";
                            command.CommandText = querylocal;
                            command.Parameters.AddWithValue("kptn", kptn);

                            MySqlDataReader dataReaderlocal = command.ExecuteReader();
                            //throw new Exception(dataReaderlocal.Read().ToString());
                            if (dataReaderlocal.Read())
                            {
                                //throw new Exception(dataReaderlocal["IsClaimed"].ToString());
                                Int32 claimed = Convert.ToInt32(dataReaderlocal["IsClaimed"]);
                                Int32 cancel = Convert.ToInt32(dataReaderlocal["IsCancelled"]);

                                if (claimed == 1)
                                {
                                    kplog.Error("FAILED:: respcod: 3, messagE: " + getRespMessage(3));
                                    dataReaderlocal.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 3, message = getRespMessage(3) };
                                }
                                if (cancel == 1)
                                {
                                    kplog.Error("FAILED:: message : " + getRespMessage(8));
                                    dataReaderlocal.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 8, message = getRespMessage(8) };
                                }
                            }

                        }



                        conn.Close();
                        kplog.Error("FAILED:: respcode: 4 , message : " + getRespMessage(4));
                        return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                    }
                }
                using (command = conn.CreateCommand())
                {
                    List<object> a = new List<object>();

                    SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();
                    //String query = "SELECT c.FirstName AS SenderFname, c.LastName AS SenderLname, c.MiddleName AS SenderMName, c.Street AS SenderStreet, c.ProvinceCity AS SenderProvCity, c.Country AS SenderCountry, c.Gender AS SenderGender, c.ContactNo AS SenderContactNo, c.IsSMS AS SenderSMS,c.Birthdate AS SenderBDate, c.BranchID AS SenderBID, c.CustID AS SenderCustID , c.MLCardNo as SenderMLCardNo, cr.CustID as ReceiverCustID, cr.FirstName AS ReceiverFname, cr.LastName AS ReceiverLName, cr.MiddleName AS ReceiverMName, cr.Street AS ReceiverStreet, cr.ProvinceCity AS ReceiverProvCity, cr.Country AS ReceiverCountry, cr.Gender AS ReceiverGender, cr.ContactNo AS ReceiverContactNo, cr.IsSMS AS ReceiverSMS, cr.Birthdate AS ReceiverBDate, cr.BranchID AS ReceiverBID, cr.MLCardNo as ReceiverMLCardNo, s.ControlNo, s.KPTNNo, s.OperatorID, s.IsPassword, s.TransPassword, s.syscreated, s.Currency, s.Principal, s.SenderID, s.SenderIsSMS, s.ReceiverID, s.ReceiverIsSMS, s.Relation, s.Message, s.StationID, s.Source, s.kptn4, s.IDType, s.IDNo, s.ExpiryDate, s.ORNo, s.Purpose, s.Charge, s.OtherCharge, s.Total FROM " + generateTableName(0) + " s INNER JOIN kpcustomersglobal.customers c ON s.SenderID = c.CustID  INNER JOIN kpcustomersglobal.customers cr ON s.ReceiverID = cr.CustID WHERE (s.KPTNNo = @kptn OR s.kptn4 = @kptn) and s.IsClaimed = 0 and s.BranchCode = @BranchCode and s.ZoneCode = @ZoneCode;";
                    String query = "SELECT IsRemote, RemoteBranch, ZoneCode, CancelCharge, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, Total, ORNo, Charge, OtherCharge, Purpose FROM " + generateTableNameGlobal(0, TransDate.ToString("yyyy-MM-dd HH:mm:ss")) + " WHERE (KPTNNo = @kptn) AND IsClaimed = 0 AND IsCancelled = 0 and IF(IsRemote, RemoteBranch, BranchCode) = @BranchCode and ZoneCode = @ZoneCode;";
                    command.CommandText = query;
                    command.Parameters.AddWithValue("kptn", kptn);
                    command.Parameters.AddWithValue("BranchCode", BranchCode);
                    command.Parameters.AddWithValue("ZoneCode", ZoneCode);
                    MySqlDataReader dataReader = command.ExecuteReader();

                    if (!dataReader.Read())
                    {
                        kplog.Error("FAILED:: respcode: 4 , message : " + getRespMessage(4));
                        dataReader.Close();
                        conn.Close();
                        return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                    }

                    bool isRemote = (bool)dataReader["IsRemote"];
                    //throw new Exception("asdf");
                    if (isRemote)
                    {
                        string remotebranch = dataReader["RemoteBranch"].ToString();

                        if (!remotebranch.Equals(BranchCode))
                        {
                            kplog.Error("FAILED:: respcode: 4 , message : " + getRespMessage(4));
                            dataReader.Close();
                            conn.Close();
                            return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                        }

                    }
                    string sFName = dataReader["SenderFname"].ToString();
                    string sLName = dataReader["SenderLname"].ToString();
                    string sMName = dataReader["SenderMName"].ToString();
                    string sSt = dataReader["SenderStreet"].ToString();
                    string sPCity = dataReader["SenderProvinceCity"].ToString();
                    string sCtry = dataReader["SenderCountry"].ToString();
                    string sG = dataReader["SenderGender"].ToString();
                    string sCNo = dataReader["SenderContactNo"].ToString();
                    Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                    string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                    string sBID = dataReader["SenderBranchID"].ToString();
                    string sCustID = dataReader["CustID"].ToString();
                    string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                    string SenderName = dataReader["SenderName"].ToString();
                    string rFName = dataReader["ReceiverFname"].ToString();
                    string rLName = dataReader["ReceiverLname"].ToString();
                    string rMName = dataReader["ReceiverMName"].ToString();
                    string rSt = dataReader["ReceiverStreet"].ToString();
                    string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                    string rCtry = dataReader["ReceiverCountry"].ToString();
                    string rG = dataReader["ReceiverGender"].ToString();
                    string rCNo = dataReader["ReceiverContactNo"].ToString();
                    string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();
                    string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                    string ReceiverName = dataReader["ReceiverName"].ToString();

                    string SendoutControlNo = dataReader["ControlNo"].ToString();
                    string KPTNNo = dataReader["KPTNNo"].ToString();
                    //string kptn4 = dataReader["kptn4"].ToString();
                    string OperatorID = dataReader["OperatorID"].ToString();
                    bool IsPassword = (bool)dataReader["IsPassword"];
                    string TransPassword = dataReader["TransPassword"].ToString();
                    DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"]);

                    string Currency = dataReader["Currency"].ToString();
                    Decimal Principal = (Decimal)dataReader["Principal"];
                    string SenderID = dataReader["CustID"].ToString();
                    Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                    string Relation = dataReader["Relation"].ToString();
                    string Message = dataReader["Message"].ToString();
                    string StationID = dataReader["StationID"].ToString();
                    string SourceOfFund = dataReader["Source"].ToString();
                    string IDType = dataReader["IDType"].ToString();
                    string IDNo = dataReader["IDNo"].ToString();

                    string ExpiryDate = (dataReader["ExpiryDate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ExpiryDate"].ToString();

                    Decimal Total = (Decimal)dataReader["Total"];
                    String ORNo = dataReader["ORNo"].ToString();
                    Decimal Charge = (Decimal)dataReader["Charge"];
                    Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                    Decimal CancelCharge = (dataReader["CancelCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["CancelCharge"];
                    string Purpose = dataReader["Purpose"].ToString();

                    dataReader.Close();
                    Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);
                    conn.Close();
                    SenderInfo si = new SenderInfo
                    {
                        FirstName = sFName,
                        LastName = sLName,
                        MiddleName = sMName,
                        SenderName = SenderName,
                        Street = sSt,
                        ProvinceCity = sPCity,
                        Country = sCtry,
                        Gender = sG,
                        ContactNo = sCNo,
                        IsSMS = sIsSM,

                        BranchID = sBID,
                        CustID = sCustID,
                        SenderMLCardNo = sMLCardNo,
                        Birthdate = sBdate
                    };

                    ReceiverInfo ri = new ReceiverInfo
                    {
                        FirstName = rFName,
                        LastName = rLName,
                        MiddleName = rMName,
                        ReceiverName = ReceiverName,
                        Street = rSt,
                        ProvinceCity = rPCity,
                        Country = rCtry,
                        Gender = rG,
                        ContactNo = rCNo,
                        BirthDate = rBdate,
                        MLCardNo = rMLCardNo
                    };

                    SendoutInfo soi = new SendoutInfo
                    {
                        SendoutControlNo = SendoutControlNo,
                        KPTNNo = KPTNNo,
                        OperatorID = OperatorID,
                        IsPassword = IsPassword,
                        TransPassword = TransPassword,
                        syscreated = syscreated,
                        Currency = Currency,
                        Principal = Principal,
                        SenderID = SenderID,
                        SenderIsSMS = SenderIsSMS,
                        Relation = Relation,
                        Message = Message,
                        StationID = StationID,
                        SourceOfFund = SourceOfFund,
                        //kptn4 = kptn4,
                        IDNo = IDNo,
                        IDType = IDType,
                        ExpiryDate = ExpiryDate,
                        DormantCharge = DormantCharge,
                        Total = Total,
                        ORNo = ORNo,
                        Charge = Charge,
                        OtherCharge = OtherCharge,
                        Purpose = Purpose,
                        CancelCharge = CancelCharge,
                        ZoneCode = ZoneCode

                    };
                    kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                    return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };

                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                conn.Close();
                return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }


    [WebMethod(BufferResponse = false)]
    public SearchResponse kptnSendoutCancelSearchAdmin(String Username, String Password, String kptn, String BranchCode, int ZoneCode, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", BranchCode: " + BranchCode + ", kptn: " + kptn + ", ZoneCode: " + ZoneCode + ", version: " + version + ", stationcode: " + stationcode);

        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new SearchResponse { respcode = 10, message = getRespMessage(10) };
        //}
        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SearchResponse { respcode = 7, message = getRespMessage(7) };
        }
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            DateTime TransDate;
            try
            {
                conn.Open();
                using (command = conn.CreateCommand())
                {
                    String query = "Select TransDate, IsClaimed, IsCancelled from " + generateTableNameGlobal(2, null) + " WHERE (KPTN6 = @kptn OR MLKP4TN = @kptn) AND IsClaimed = 0 AND IsCancelled = 0;";
                    command.CommandText = query;

                    command.Parameters.AddWithValue("kptn", kptn);
                    MySqlDataReader dataReader = command.ExecuteReader();
                    if (dataReader.Read())
                    {

                        //throw new Exception(dataReader["TransDate"].GetType().ToString());
                        TransDate = Convert.ToDateTime(dataReader["TransDate"]);
                        dataReader.Close();
                    }
                    else
                    {
                        dataReader.Close();

                        using (command = conn.CreateCommand())
                        {
                            String querylocal = "Select TransDate, IsClaimed, IsCancelled from " + generateTableNameGlobal(2, null) + " WHERE (KPTN6 = @kptn OR MLKP4TN = @kptn);";
                            command.CommandText = querylocal;
                            command.Parameters.AddWithValue("kptn", kptn);

                            MySqlDataReader dataReaderlocal = command.ExecuteReader();
                            //throw new Exception(dataReaderlocal.Read().ToString());
                            if (dataReaderlocal.Read())
                            {
                                //throw new Exception(dataReaderlocal["IsClaimed"].ToString());
                                Int32 claimed = Convert.ToInt32(dataReaderlocal["IsClaimed"]);
                                Int32 cancel = Convert.ToInt32(dataReaderlocal["IsCancelled"]);

                                if (claimed == 1)
                                {
                                    kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(3)); 
                                    dataReaderlocal.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 3, message = getRespMessage(3) };
                                }
                                if (cancel == 1)
                                {
                                    kplog.Error("FAILED:: respcode: 8, message: " + getRespMessage(8));
                                    dataReaderlocal.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 8, message = getRespMessage(8) };
                                }
                            }

                        }



                        conn.Close();
                        kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                        return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                    }
                }
                using (command = conn.CreateCommand())
                {
                    List<object> a = new List<object>();

                    SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();
                    //String query = "SELECT c.FirstName AS SenderFname, c.LastName AS SenderLname, c.MiddleName AS SenderMName, c.Street AS SenderStreet, c.ProvinceCity AS SenderProvCity, c.Country AS SenderCountry, c.Gender AS SenderGender, c.ContactNo AS SenderContactNo, c.IsSMS AS SenderSMS,c.Birthdate AS SenderBDate, c.BranchID AS SenderBID, c.CustID AS SenderCustID , c.MLCardNo as SenderMLCardNo, cr.CustID as ReceiverCustID, cr.FirstName AS ReceiverFname, cr.LastName AS ReceiverLName, cr.MiddleName AS ReceiverMName, cr.Street AS ReceiverStreet, cr.ProvinceCity AS ReceiverProvCity, cr.Country AS ReceiverCountry, cr.Gender AS ReceiverGender, cr.ContactNo AS ReceiverContactNo, cr.IsSMS AS ReceiverSMS, cr.Birthdate AS ReceiverBDate, cr.BranchID AS ReceiverBID, cr.MLCardNo as ReceiverMLCardNo, s.ControlNo, s.KPTNNo, s.OperatorID, s.IsPassword, s.TransPassword, s.syscreated, s.Currency, s.Principal, s.SenderID, s.SenderIsSMS, s.ReceiverID, s.ReceiverIsSMS, s.Relation, s.Message, s.StationID, s.Source, s.kptn4, s.IDType, s.IDNo, s.ExpiryDate, s.ORNo, s.Purpose, s.Charge, s.OtherCharge, s.Total FROM " + generateTableName(0) + " s INNER JOIN kpcustomersglobal.customers c ON s.SenderID = c.CustID  INNER JOIN kpcustomersglobal.customers cr ON s.ReceiverID = cr.CustID WHERE (s.KPTNNo = @kptn OR s.kptn4 = @kptn) and s.IsClaimed = 0 and s.BranchCode = @BranchCode and s.ZoneCode = @ZoneCode;";
                    String query = "SELECT IsRemote, Reason, RemoteBranch, BranchCode, RemoteOperatorID, ZoneCode, CancelCharge, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, Total, ORNo, Charge, OtherCharge, Purpose FROM " + generateTableNameGlobal(0, TransDate.ToString("yyyy-MM-dd HH:mm:ss")) + " WHERE (KPTNNo = @kptn) AND IsClaimed = 0 AND IsCancelled = 0 ORDER BY transdate DESC LIMIT 1;";
                    command.CommandText = query;
                    command.Parameters.AddWithValue("kptn", kptn);

                    MySqlDataReader dataReader = command.ExecuteReader();

                    if (!dataReader.Read())
                    {
                        kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                        dataReader.Close();
                        conn.Close();
                        return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                    }

                    bool isRemote = (bool)dataReader["IsRemote"];
                    //throw new Exception("asdf");
                    //if (isRemote)
                    //{
                    //    string remotebranch = dataReader["RemoteBranch"].ToString();

                    //    if (!remotebranch.Equals(BranchCode))
                    //    {
                    //        dataReader.Close();
                    //        conn.Close();
                    //        return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                    //    }

                    //}
                    string sFName = dataReader["SenderFname"].ToString();
                    string sLName = dataReader["SenderLname"].ToString();
                    string sMName = dataReader["SenderMName"].ToString();
                    string sSt = dataReader["SenderStreet"].ToString();
                    string sPCity = dataReader["SenderProvinceCity"].ToString();
                    string sCtry = dataReader["SenderCountry"].ToString();
                    string sG = dataReader["SenderGender"].ToString();
                    string sCNo = dataReader["SenderContactNo"].ToString();
                    Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                    string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                    string sBID = dataReader["SenderBranchID"].ToString();
                    string sCustID = dataReader["CustID"].ToString();
                    string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                    string SenderName = dataReader["SenderName"].ToString();
                    string rFName = dataReader["ReceiverFname"].ToString();
                    string rLName = dataReader["ReceiverLname"].ToString();
                    string rMName = dataReader["ReceiverMName"].ToString();
                    string rSt = dataReader["ReceiverStreet"].ToString();
                    string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                    string rCtry = dataReader["ReceiverCountry"].ToString();
                    string rG = dataReader["ReceiverGender"].ToString();
                    string rCNo = dataReader["ReceiverContactNo"].ToString();
                    string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();
                    string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                    string ReceiverName = dataReader["ReceiverName"].ToString();

                    string SendoutControlNo = dataReader["ControlNo"].ToString();
                    string KPTNNo = dataReader["KPTNNo"].ToString();
                    //string kptn4 = dataReader["kptn4"].ToString();
                    string OperatorID = dataReader["OperatorID"].ToString();
                    bool IsPassword = (bool)dataReader["IsPassword"];
                    string TransPassword = dataReader["TransPassword"].ToString();
                    DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"]);

                    string Currency = dataReader["Currency"].ToString();
                    Decimal Principal = (Decimal)dataReader["Principal"];
                    string SenderID = dataReader["CustID"].ToString();
                    Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                    string Relation = dataReader["Relation"].ToString();
                    string Message = dataReader["Message"].ToString();
                    string StationID = dataReader["StationID"].ToString();
                    string SourceOfFund = dataReader["Source"].ToString();
                    string IDType = dataReader["IDType"].ToString();
                    string IDNo = dataReader["IDNo"].ToString();

                    string ExpiryDate = (dataReader["ExpiryDate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ExpiryDate"].ToString();

                    Decimal Total = (Decimal)dataReader["Total"];
                    String ORNo = dataReader["ORNo"].ToString();
                    Decimal Charge = (Decimal)dataReader["Charge"];
                    Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                    Decimal CancelCharge = (dataReader["CancelCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["CancelCharge"];
                    string Purpose = dataReader["Purpose"].ToString();
                    string RemoteBranch = dataReader["RemoteBranch"].ToString();
                    string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                    string BCode = dataReader["BranchCode"].ToString();
                    string Reason = dataReader["Reason"].ToString();
                    dataReader.Close();
                    Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);
                    conn.Close();
                    SenderInfo si = new SenderInfo
                    {
                        FirstName = sFName,
                        LastName = sLName,
                        MiddleName = sMName,
                        SenderName = SenderName,
                        Street = sSt,
                        ProvinceCity = sPCity,
                        Country = sCtry,
                        Gender = sG,
                        ContactNo = sCNo,
                        IsSMS = sIsSM,

                        BranchID = sBID,
                        CustID = sCustID,
                        SenderMLCardNo = sMLCardNo,
                        Birthdate = sBdate
                    };

                    ReceiverInfo ri = new ReceiverInfo
                    {
                        FirstName = rFName,
                        LastName = rLName,
                        MiddleName = rMName,
                        ReceiverName = ReceiverName,
                        Street = rSt,
                        ProvinceCity = rPCity,
                        Country = rCtry,
                        Gender = rG,
                        ContactNo = rCNo,
                        BirthDate = rBdate,
                        MLCardNo = rMLCardNo
                    };

                    SendoutInfo soi = new SendoutInfo
                    {
                        SendoutControlNo = SendoutControlNo,
                        KPTNNo = KPTNNo,
                        OperatorID = OperatorID,
                        IsPassword = IsPassword,
                        TransPassword = TransPassword,
                        syscreated = syscreated,
                        Currency = Currency,
                        Principal = Principal,
                        SenderID = SenderID,
                        SenderIsSMS = SenderIsSMS,
                        Relation = Relation,
                        Message = Message,
                        StationID = StationID,
                        SourceOfFund = SourceOfFund,
                        //kptn4 = kptn4,
                        IDNo = IDNo,
                        IDType = IDType,
                        ExpiryDate = ExpiryDate,
                        DormantCharge = DormantCharge,
                        Total = Total,
                        ORNo = ORNo,
                        Charge = Charge,
                        OtherCharge = OtherCharge,
                        Purpose = Purpose,
                        CancelCharge = CancelCharge,
                        ZoneCode = ZoneCode,
                        IsRemote = isRemote,
                        RemoteBranch = RemoteBranch,
                        RemoteOperatorID = RemoteOperatorID,
                        BranchCode = BCode,
                        RemoteReason = Reason



                    };
                    kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                    return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };

                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                conn.Close();
                return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
            }
        }
    }



    [WebMethod]
    public RePrintResponse saveReprintGlobal(String Username, String Password, String KPTNNo, String OperatorID, String reprintBcode, Int32 reprintZcode, String Reason, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", KPTNNo: " + KPTNNo + ", OperatorID: " + OperatorID+", reprintBcode: " + reprintBcode + ", reprintZcode: " + reprintZcode + ", Reason: " + Reason + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new RePrintResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new RePrintResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                conn.Open();
                using (command = conn.CreateCommand())
                {
                    dt = getServerDateGlobal(true);
                    command.CommandText = "Insert into kpglobal.reprint (`KPTNNo`,`ReprintDate`,`OperatorID`,`BranchCode`,`ZoneCode`, `Reason`) values (@KPTNNo,@ReprintDate,@OperatorID,@BranchCode,@ZoneCode,@Reason)";
                    command.Parameters.AddWithValue("KPTNNo", KPTNNo);
                    //command.Parameters.AddWithValue("kptn4", kptn4);
                    command.Parameters.AddWithValue("ReprintDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    command.Parameters.AddWithValue("OperatorID", OperatorID);
                    command.Parameters.AddWithValue("BranchCode", reprintBcode);
                    command.Parameters.AddWithValue("ZoneCode", reprintZcode);
                    command.Parameters.AddWithValue("Reason", Reason);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message : Insert into kpglobal.reprint: "
                     + " KPTNNo: " + KPTNNo
                     + " ReprintDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                     + " OperatorID: " + OperatorID
                     + " BranchCode: " + reprintBcode
                     + " ZoneCode: " + reprintZcode
                     + " Reason: " + Reason);
                    conn.Close();
                    kplog.Info("SUCCES:: respocode: 1 , message : " + getRespMessage(1));
                    return new RePrintResponse { respcode = 1, message = getRespMessage(1) };
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                conn.Close();
                return new RePrintResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }

    }


    [WebMethod]
    public RePrintResponse saveReprintDomestic(String Username, String Password, String KPTNNo, String OperatorID, String reprintBcode, Int32 reprintZcode, String Reason, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", KPTNNo: " + KPTNNo + ", OperatorID: " + OperatorID + ", reprintBcode: " + reprintBcode + ", reprintZcode: " + reprintZcode + ", Reason: " + Reason + ", version: " + version + ", stationcode: " + stationcode);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new RePrintResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new RePrintResponse { respcode = 10, message = getRespMessage(10) };
        //}
        using (MySqlConnection conn = dbconDomestic.getConnection())
        {
            try
            {
                conn.Open();
                using (command = conn.CreateCommand())
                {
                    dt = getServerDateGlobal(true);
                    command.CommandText = "Insert into kpdomestic.reprint (`KPTNNo`,`ReprintDate`,`OperatorID`,`BranchCode`,`ZoneCode`, `Reason`) values (@KPTNNo,@ReprintDate,@OperatorID,@BranchCode,@ZoneCode,@Reason)";
                    command.Parameters.AddWithValue("KPTNNo", KPTNNo);
                    //command.Parameters.AddWithValue("kptn4", kptn4);
                    command.Parameters.AddWithValue("ReprintDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    command.Parameters.AddWithValue("OperatorID", OperatorID);
                    command.Parameters.AddWithValue("BranchCode", reprintBcode);
                    command.Parameters.AddWithValue("ZoneCode", reprintZcode);
                    command.Parameters.AddWithValue("Reason", Reason);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message : Insert into kpdomestic.reprint "
                    + " ReprintDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                    + " OperatorID: " + OperatorID
                    + " BranchCode: " + reprintBcode
                    + " ZoneCode: " + reprintZcode
                    + " Reason: " + Reason);
                    conn.Close();
                    kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1));
                    return new RePrintResponse { respcode = 1, message = getRespMessage(1) };
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                conn.Close();
                return new RePrintResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
        }

    }

    [WebMethod(BufferResponse = false)]
    public SearchResponse rePrintGlobalD2b(String Username, String Password, String kptn, Int32 type, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn: " + kptn + ", type: " + type + ", version: " + version + ", stationcode: " + stationcode );

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new SearchResponse { respcode = 7, message = getRespMessage(7) };
            }

            if (type > 2 || 2 < 0)
            {
                kplog.Error("FAILED:: respcode: 0 , message : " + getRespMessage(0) + ", ErrorDetail: Type must not be greater or less than 1 and 0");
                return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = "Type must not be greater or less than 1 and 0" };
            }
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                DateTime ClaimDate = DateTime.Now;
                try
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        List<object> a = new List<object>();

                        SendoutInfo soi;
                        SenderInfo si;
                        ReceiverInfo ri;
                        PayoutInfo poi;
                        if (type == 2)
                        {
                            if (decodeKPTNGlobald2b(0, kptn) == "4")
                            {
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 0 , message: Invalid KPTN!");
                                return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                            }
                            //String qrychktrxn = "SELECT CompletedDate, RejectedDate FROM " + decodeKPTNGlobald2b(0, kptn) + " WHERE KPTNNo = @kptn1 and (CancelledDate is null or CancelledDate = ' ')";
                            //command.CommandText = qrychktrxn;
                            //command.Parameters.Clear();
                            //command.Parameters.AddWithValue("kptn1", kptn);
                            //MySqlDataReader rdrchktrxn = command.ExecuteReader();
                            //if (rdrchktrxn.HasRows)
                            //{
                            //    rdrchktrxn.Read();
                            //    if ((rdrchktrxn["CompletedDate"] != DBNull.Value || rdrchktrxn["CompletedDate"].ToString() != String.Empty) && (rdrchktrxn["RejectedDate"] == DBNull.Value || rdrchktrxn["RejectedDate"].ToString() == String.Empty))
                            //    {
                            //        rdrchktrxn.Close();
                            //        conn.Close();
                            //        return new SearchResponse { respcode = 0, message = "Transaction is already Claimed" };
                            //    }
                            //}
                            //else
                            //{
                            //    rdrchktrxn.Close();
                            //    conn.Close();
                            //    return new SearchResponse { respcode = 0, message = "KPTN not found" };
                            //}
                            //rdrchktrxn.Close();

                            String query = "SELECT ControlNo, KPTNNo, ORNo, IRNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, IsPassword, TransPassword, Purpose, OLDKPTNNo, syscreated, syscreator, sysmodified, sysmodifier, Source, PreferredCurrency, Principal, Charge, OtherCharge, Redeem, Total, Promo, SenderIsSMS, Relation, Message, IDType, IDNo, ExpiryDate, CancelledDate, BranchCode, ZoneCode, TransDate, CancelledByOperatorID, CancelledByBranchCode, CancelledByZoneCode, CancelledByStationID, CancelReason, CancelDetails, SenderMLCardNo, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderBirthdate, SenderBranchID, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthDate, CancelCharge, ChargeTo,remotezonecode,exchangerate,vat,CompletedDate,RejectedDate,SenderBankAccount,SenderBankCode FROM " + decodeKPTNGlobald2b(0, kptn) + " WHERE KPTNNo = @kptn;";
                            command.CommandText = query;
                            command.Parameters.AddWithValue("kptn", kptn);
                            MySqlDataReader dataReader = command.ExecuteReader();

                            if (dataReader.HasRows)
                            {
                                dataReader.Read();
                                string sFName = dataReader["SenderFname"].ToString();
                                string sLName = dataReader["SenderLname"].ToString();
                                string sMName = dataReader["SenderMName"].ToString();
                                string sSt = dataReader["SenderStreet"].ToString();
                                string sPCity = dataReader["SenderProvinceCity"].ToString();
                                string sCtry = dataReader["SenderCountry"].ToString();
                                string sG = dataReader["SenderGender"].ToString();
                                string sCNo = dataReader["SenderContactNo"].ToString();
                                Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                                string sBID = dataReader["SenderBranchID"].ToString();
                                string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                                string SenderName = dataReader["SenderName"].ToString();
                                string rFName = dataReader["ReceiverFname"].ToString();
                                string rLName = dataReader["ReceiverLname"].ToString();
                                string rMName = dataReader["ReceiverMName"].ToString();
                                string rSt = dataReader["ReceiverStreet"].ToString();
                                string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                                string rCtry = dataReader["ReceiverCountry"].ToString();
                                string rG = dataReader["ReceiverGender"].ToString();
                                string rCNo = dataReader["ReceiverContactNo"].ToString();
                                string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();
                                string ReceiverName = dataReader["ReceiverName"].ToString();
                                string SendoutControlNo = dataReader["ControlNo"].ToString();
                                string KPTNNo = dataReader["KPTNNo"].ToString();
                                string OperatorID = dataReader["OperatorID"].ToString();
                                Boolean IsPassword = Convert.ToBoolean(dataReader["IsPassword"]);
                                string TransPassword = dataReader["TransPassword"].ToString();
                                DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"].ToString());
                                string Currency = dataReader["PreferredCurrency"].ToString();
                                Decimal Principal = (Decimal)dataReader["Principal"];
                                Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string Relation = dataReader["Relation"].ToString();
                                string Message = dataReader["Message"].ToString();
                                string StationID = dataReader["StationID"].ToString();
                                string SourceOfFund = dataReader["Source"].ToString();
                                string IDType = dataReader["IDType"].ToString();
                                string IDNo = dataReader["IDNo"].ToString();
                                string ExpiryDate = dataReader["ExpiryDate"].ToString();
                                string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                                string ControlNo = dataReader["ControlNo"].ToString();
                                string BranchCode = dataReader["BranchCode"].ToString();
                                Decimal Redeem = (Decimal)dataReader["Redeem"];
                                string ORNo = dataReader["ORNo"].ToString();
                                Decimal Total = (Decimal)dataReader["Total"];
                                Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                                Decimal Charge = (Decimal)dataReader["Charge"];
                                string Purpose = dataReader["Purpose"].ToString();
                                Int32 ZoneCode = Convert.ToInt32(dataReader["ZoneCode"]);
                                Boolean IsRemote = Convert.ToBoolean(dataReader["IsRemote"]);
                                Decimal CancelCharge = (dataReader["CancelCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["CancelCharge"];
                                String reason = dataReader["Reason"].ToString();
                                String cancelReason = dataReader["CancelReason"].ToString();
                                Int32 remotezone = Convert.ToInt32(dataReader["remotezonecode"]);
                                Decimal exchangerate = (Decimal)dataReader["exchangerate"];
                                Double vat = Convert.ToDouble(dataReader["vat"]);
                                string completed = dataReader["CompletedDate"].ToString();
                                string rejected = dataReader["RejectedDate"].ToString();
                                string sndbankaccount = dataReader["SenderBankAccount"].ToString();
                                string sndbankcode = dataReader["SenderBankCode"].ToString();

                                dataReader.Close();

                                String pzipcode = String.Empty;
                                String phomecity = String.Empty;

                                command.Parameters.Clear();
                                command.CommandText = "SELECT c.ZipCode, cd.HomeCity FROM kpcustomersglobal.customers c INNER JOIN kpcustomersglobal.customersdetails cd ON cd.CustID = c.CustID WHERE c.FirstName=@xfname AND c.LastName=@xlname AND c.MiddleName=@xmname";
                                command.Parameters.AddWithValue("xfname", sFName);
                                command.Parameters.AddWithValue("xlname", sLName);
                                command.Parameters.AddWithValue("xmname", sMName);
                                MySqlDataReader rdrgetcust = command.ExecuteReader();
                                if (rdrgetcust.Read())
                                {
                                    pzipcode = rdrgetcust["ZipCode"].ToString();
                                    phomecity = rdrgetcust["HomeCity"].ToString();
                                }
                                rdrgetcust.Close();

                                Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);

                                command.Transaction = trans;
                                command.Parameters.Clear();
                                command.CommandText = "kpadminlogsglobal.savelog53";
                                command.CommandType = CommandType.StoredProcedure;
                                command.Parameters.AddWithValue("kptnno", KPTNNo);
                                command.Parameters.AddWithValue("action", "SO REPRINT");
                                command.Parameters.AddWithValue("isremote", IsRemote);
                                command.Parameters.AddWithValue("txndate", syscreated);
                                command.Parameters.AddWithValue("stationcode", stationcode);
                                command.Parameters.AddWithValue("stationno", StationID);
                                command.Parameters.AddWithValue("zonecode", ZoneCode);
                                command.Parameters.AddWithValue("branchcode", BranchCode);
                                command.Parameters.AddWithValue("operatorid", OperatorID);
                                command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                                command.Parameters.AddWithValue("remotereason", reason);
                                command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                                command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                                command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                                command.Parameters.AddWithValue("remotezonecode", remotezone);
                                command.Parameters.AddWithValue("type", "N");
                                try
                                {
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : stored proc: kpadminlogsglobal.savelog53:  "
                                + " kptnno: " + KPTNNo
                                + " action: " + "SO REPRINT"
                                + " isremote: " + IsRemote
                                + " txndate: " + syscreated
                                + " stationcode: " + stationcode
                                + " stationno: " + StationID
                                + " zonecode: " + ZoneCode
                                + " branchcode: " + BranchCode
                                + " operatorid: " + OperatorID
                                + " cancelledreason: " + DBNull.Value
                                + " remotereason: " + reason
                                + " remotebranch: " + DBNull.Value
                                + " remoteoperator: " + DBNull.Value
                                + " oldkptnno: " + DBNull.Value
                                + " remotezonecode: " + remotezone
                                + " type: " + "N");
                                    trans.Commit();
                                    conn.Close();
                                }
                                catch (MySqlException ex)
                                {
                                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                                    trans.Rollback();
                                    conn.Close();
                                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                                }

                                si = new SenderInfo
                                {
                                    FirstName = sFName,
                                    LastName = sLName,
                                    MiddleName = sMName,
                                    SenderName = SenderName,
                                    Street = sSt,
                                    ProvinceCity = sPCity,
                                    Country = sCtry,
                                    Gender = sG,
                                    ContactNo = sCNo,
                                    IsSMS = sIsSM,
                                    BranchID = sBID,
                                    SenderMLCardNo = sMLCardNo,
                                    zipcode = pzipcode,
                                    homecity = phomecity
                                };

                                ri = new ReceiverInfo
                                {
                                    FirstName = rFName,
                                    LastName = rLName,
                                    MiddleName = rMName,
                                    ReceiverName = ReceiverName,
                                    Street = rSt,
                                    ProvinceCity = rPCity,
                                    Country = rCtry,
                                    Gender = rG,
                                    ContactNo = rCNo,
                                    BirthDate = rBdate,
                                };

                                soi = new SendoutInfo
                                {
                                    SendoutControlNo = SendoutControlNo,
                                    KPTNNo = KPTNNo,
                                    OperatorID = OperatorID,
                                    IsPassword = IsPassword,
                                    TransPassword = TransPassword,
                                    syscreated = syscreated,
                                    Currency = Currency,
                                    Principal = Principal,
                                    SenderIsSMS = SenderIsSMS,
                                    Relation = Relation,
                                    Message = Message,
                                    StationID = StationID,
                                    SourceOfFund = SourceOfFund,
                                    IDNo = IDNo,
                                    IDType = IDType,
                                    ExpiryDate = ExpiryDate,
                                    DormantCharge = DormantCharge,
                                    RemoteOperatorID = RemoteOperatorID,
                                    RemoteBranch = RemoteBranch,
                                    BranchCode = BranchCode,
                                    Redeem = Redeem,
                                    ORNo = ORNo,
                                    Charge = Charge,
                                    OtherCharge = OtherCharge,
                                    Purpose = Purpose,
                                    Total = Total,
                                    ZoneCode = ZoneCode,
                                    IsRemote = IsRemote,
                                    CancelCharge = CancelCharge,
                                    RemoteReason = reason,
                                    RemoteZone = remotezone,
                                    xchangerate = exchangerate,
                                    vat = vat,
                                    senderbankaccount = sndbankaccount,
                                    senderbankcode = sndbankcode
                                };
                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                                return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                            }
                        }
                    }

                    conn.Close();
                    kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + null + ", RecieverInfo: " + null + ", SendoutInfo: " + null);
                    return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
                catch (Exception ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    conn.Close();
                    if (ex.Message.Equals("4"))
                    {
                        kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                        return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                    }
                    return new SearchResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            return new SearchResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
        }
    }

    [WebMethod(BufferResponse = false)]
    public SearchResponse rePrintGlobal(String Username, String Password, String kptn, Int32 type, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn: " + kptn + ", type: " + type + ", version: " + version + ", stationcode: " + stationcode);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new SearchResponse { respcode = 7, message = getRespMessage(7) };
            }

            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new SearchResponse { respcode = 10, message = getRespMessage(10) };
            //}
            if (type > 1 || 1 < 0)
            {
                kplog.Error("FAILED:: respcode: 0 , message : " + getRespMessage(0) + ", ErrorDetail: Type must not be greater or less than 1 and 0");
                return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = "Type must not be greater or less than 1 and 0" };
            }
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                //DateTime TransDate;
                DateTime ClaimDate = DateTime.Now;
                //Boolean isClaimed;
                try
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        List<object> a = new List<object>();

                        //SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();
                        //String query = "SELECT SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, kptn4, IDType, IDNo, ExpiryDate, SenderName, ReceiverName FROM " + generateTableName(2) + " as t INNER JOIN ON " + generateTableName(0) + " s ON t.KPTN6 = s.KPTNNo and t.MLKP4TN = s.kptn4 WHERE (MLKP4TN = @kptn OR MLKP4TN = @kptn) and IsClaimed = 0;";
                        SendoutInfo soi;
                        SenderInfo si;
                        ReceiverInfo ri;
                        PayoutInfo poi;
                        if (type == 0)
                        {
                            //String query = "SELECT SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, kptn4, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, RemoteBranch, RemoteOperatorID, ControlNo, BranchCode, Redeem, ORNo FROM " + generateTableName(0, TransDate.ToString("yyyy-MM-dd HH:mm:ss")) + " WHERE (KPTNNo = @kptn OR kptn4 = @kptn);";
                            if ((decodeKPTNGlobal(0, kptn)) != "4")
                            {
                                String query = "SELECT ControlNo, KPTNNo, ORNo, IRNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, IsPassword, TransPassword, Purpose, OLDKPTNNo, IsClaimed, IsCancelled, syscreated, syscreator, sysmodified, sysmodifier, Source, PreferredCurrency, Principal, Charge, OtherCharge, Redeem, Total, Promo, SenderIsSMS, Relation, Message, IDType, IDNo, ExpiryDate, CancelledDate, BranchCode, ZoneCode, TransDate, CancelledByOperatorID, CancelledByBranchCode, CancelledByZoneCode, CancelledByStationID, CancelReason, CancelDetails, SenderMLCardNo, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderBirthdate, SenderBranchID, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthDate, CancelCharge, ChargeTo,remotezonecode,exchangerate,TransType FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn;";
                                command.CommandText = query;
                                command.Parameters.AddWithValue("kptn", kptn);
                            }
                            else
                            {
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 0 , message: Invalid KPTN!");
                                return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                            }
                            MySqlDataReader dataReader = command.ExecuteReader();

                            if (dataReader.HasRows)
                            {
                                dataReader.Read();
                                string sFName = dataReader["SenderFname"].ToString();
                                string sLName = dataReader["SenderLname"].ToString();
                                string sMName = dataReader["SenderMName"].ToString();
                                string sSt = dataReader["SenderStreet"].ToString();
                                string sPCity = dataReader["SenderProvinceCity"].ToString();
                                string sCtry = dataReader["SenderCountry"].ToString();
                                string sG = dataReader["SenderGender"].ToString();
                                string sCNo = dataReader["SenderContactNo"].ToString();
                                Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                                string sBID = dataReader["SenderBranchID"].ToString();
                                //string sCustID = dataReader["CustID"].ToString();
                                string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                                string SenderName = dataReader["SenderName"].ToString();

                                string rFName = dataReader["ReceiverFname"].ToString();
                                string rLName = dataReader["ReceiverLname"].ToString();
                                string rMName = dataReader["ReceiverMName"].ToString();
                                string rSt = dataReader["ReceiverStreet"].ToString();
                                string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                                string rCtry = dataReader["ReceiverCountry"].ToString();
                                string rG = dataReader["ReceiverGender"].ToString();
                                string rCNo = dataReader["ReceiverContactNo"].ToString();
                                string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                                //string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                                string ReceiverName = dataReader["ReceiverName"].ToString();

                                string SendoutControlNo = dataReader["ControlNo"].ToString();
                                string KPTNNo = dataReader["KPTNNo"].ToString();
                                //string kptn4 = dataReader["kptn4"].ToString();
                                string OperatorID = dataReader["OperatorID"].ToString();
                                Boolean IsPassword = Convert.ToBoolean(dataReader["IsPassword"]);
                                string TransPassword = dataReader["TransPassword"].ToString();
                                DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"].ToString());
                                string Currency = dataReader["PreferredCurrency"].ToString();
                                Decimal Principal = (Decimal)dataReader["Principal"];
                                //string SenderID = dataReader["CustID"].ToString();
                                Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string Relation = dataReader["Relation"].ToString();
                                string Message = dataReader["Message"].ToString();
                                string StationID = dataReader["StationID"].ToString();
                                string SourceOfFund = dataReader["Source"].ToString();
                                string IDType = dataReader["IDType"].ToString();
                                string IDNo = dataReader["IDNo"].ToString();
                                string ExpiryDate = dataReader["ExpiryDate"].ToString();
                                //RemoteBranch, RemoteOperatorID, IDType, IDNo, ExpiryDate,ControlNo
                                string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                                string ControlNo = dataReader["ControlNo"].ToString();
                                string BranchCode = dataReader["BranchCode"].ToString();
                                Decimal Redeem = (Decimal)dataReader["Redeem"];
                                string ORNo = dataReader["ORNo"].ToString();
                                Decimal Total = (Decimal)dataReader["Total"];
                                Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                                Decimal Charge = (Decimal)dataReader["Charge"];
                                string Purpose = dataReader["Purpose"].ToString();
                                Int32 ZoneCode = Convert.ToInt32(dataReader["ZoneCode"]);
                                Boolean IsRemote = Convert.ToBoolean(dataReader["IsRemote"]);
                                Decimal CancelCharge = (dataReader["CancelCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["CancelCharge"];
                                Boolean x = Convert.ToBoolean(dataReader["IsCancelled"]);
                                Boolean IsClaimed = Convert.ToBoolean(dataReader["IsClaimed"]);
                                String reason = dataReader["Reason"].ToString();
                                String cancelReason = dataReader["CancelReason"].ToString();
                                Int32 remotezone = Convert.ToInt32(dataReader["remotezonecode"]);
                                Decimal exchangerate = (Decimal)dataReader["exchangerate"];
                                String trxntype = dataReader["TransType"].ToString();
                                // BETA for testing
                                //if (x && !cancelReason.Equals("Return to Sender"))
                                //{
                                //    dataReader.Close();
                                //    conn.Close();
                                //    return new SearchResponse { respcode = 8, message = getRespMessage(8), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                //}

                                //if (!IsClaimed)
                                //{
                                //    dataReader.Close();
                                //    conn.Close();
                                //    return new SearchResponse { respcode = 3, message = getRespMessage(9), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                //}

                                //string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                //string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();

                                dataReader.Close();

                                String pzipcode = String.Empty;
                                String phomecity = String.Empty;

                                command.Parameters.Clear();
                                command.CommandText = "SELECT c.ZipCode, cd.HomeCity FROM kpcustomersglobal.customers c INNER JOIN kpcustomersglobal.customersdetails cd ON cd.CustID = c.CustID WHERE c.FirstName=@xfname AND c.LastName=@xlname AND c.MiddleName=@xmname";
                                command.Parameters.AddWithValue("xfname", sFName);
                                command.Parameters.AddWithValue("xlname", sLName);
                                command.Parameters.AddWithValue("xmname", sMName);
                                MySqlDataReader rdrgetcust = command.ExecuteReader();
                                if (rdrgetcust.Read())
                                {
                                    pzipcode = rdrgetcust["ZipCode"].ToString();
                                    phomecity = rdrgetcust["HomeCity"].ToString();
                                }
                                rdrgetcust.Close();

                                Decimal DormantCharge = CalculateDormantChargeGlobal(syscreated);


                                command.Transaction = trans;
                                command.Parameters.Clear();
                                command.CommandText = "kpadminlogsglobal.savelog53";
                                command.CommandType = CommandType.StoredProcedure;

                                command.Parameters.AddWithValue("kptnno", KPTNNo);
                                command.Parameters.AddWithValue("action", "SO REPRINT");
                                command.Parameters.AddWithValue("isremote", IsRemote);
                                command.Parameters.AddWithValue("txndate", syscreated);
                                command.Parameters.AddWithValue("stationcode", stationcode);
                                command.Parameters.AddWithValue("stationno", StationID);
                                command.Parameters.AddWithValue("zonecode", ZoneCode);
                                command.Parameters.AddWithValue("branchcode", BranchCode);
                                //command.Parameters.AddWithValue("branchname", DBNull.Value);
                                command.Parameters.AddWithValue("operatorid", OperatorID);
                                command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                                command.Parameters.AddWithValue("remotereason", reason);
                                command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                                command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                                command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                                command.Parameters.AddWithValue("remotezonecode", remotezone);
                                command.Parameters.AddWithValue("type", "N");
                                try
                                {
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message : stored proc: kpadminlogsglobal.savelog53:  "
                                + " kptnno: " + KPTNNo
                                + " action: " + "SO REPRINT"
                                + " isremote: " + IsRemote
                                + " txndate: " + syscreated
                                + " stationcode: " + stationcode
                                + " stationno: " + StationID
                                + " zonecode: " + ZoneCode
                                + " branchcode: " + BranchCode
                                + " operatorid: " + OperatorID
                                + " cancelledreason: " + DBNull.Value
                                + " remotereason: " + reason
                                + " remotebranch: " + DBNull.Value
                                + " remoteoperator: " + DBNull.Value
                                + " oldkptnno: " + DBNull.Value
                                + " remotezonecode: " + remotezone
                                + " type: " + "N");
                                    trans.Commit();
                                    conn.Close();
                                }
                                catch (MySqlException ex)
                                {
                                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                                    trans.Rollback();
                                    conn.Close();
                                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                                }

                                si = new SenderInfo
                                {
                                    FirstName = sFName,
                                    LastName = sLName,
                                    MiddleName = sMName,
                                    SenderName = SenderName,
                                    Street = sSt,
                                    ProvinceCity = sPCity,
                                    Country = sCtry,
                                    Gender = sG,
                                    ContactNo = sCNo,
                                    IsSMS = sIsSM,
                                    BranchID = sBID,
                                    //CustID = sCustID,
                                    SenderMLCardNo = sMLCardNo,
                                    zipcode = pzipcode,
                                    homecity = phomecity
                                };

                                ri = new ReceiverInfo
                                {
                                    FirstName = rFName,
                                    LastName = rLName,
                                    MiddleName = rMName,
                                    ReceiverName = ReceiverName,
                                    Street = rSt,
                                    ProvinceCity = rPCity,
                                    Country = rCtry,
                                    Gender = rG,
                                    ContactNo = rCNo,
                                    BirthDate = rBdate,
                                    //MLCardNo = rMLCardNo
                                };

                                soi = new SendoutInfo
                                {
                                    SendoutControlNo = SendoutControlNo,
                                    KPTNNo = KPTNNo,
                                    OperatorID = OperatorID,
                                    IsPassword = IsPassword,
                                    TransPassword = TransPassword,
                                    syscreated = syscreated,
                                    Currency = Currency,
                                    Principal = Principal,
                                    //SenderID = SenderID,
                                    SenderIsSMS = SenderIsSMS,
                                    Relation = Relation,
                                    Message = Message,
                                    StationID = StationID,
                                    SourceOfFund = SourceOfFund,
                                    //kptn4 = kptn4,
                                    IDNo = IDNo,
                                    IDType = IDType,
                                    ExpiryDate = ExpiryDate,
                                    DormantCharge = DormantCharge,
                                    RemoteOperatorID = RemoteOperatorID,
                                    RemoteBranch = RemoteBranch,
                                    BranchCode = BranchCode,
                                    Redeem = Redeem,
                                    ORNo = ORNo,
                                    Charge = Charge,
                                    OtherCharge = OtherCharge,
                                    Purpose = Purpose,
                                    Total = Total,
                                    ZoneCode = ZoneCode,
                                    IsRemote = IsRemote,
                                    CancelCharge = CancelCharge,
                                    RemoteReason = reason,
                                    RemoteZone = remotezone,
                                    xchangerate = exchangerate
                                };
                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                                return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                            }
                        }
                        else
                        {
                            using (command = conn.CreateCommand())
                            {
                                if ((decodeKPTNGlobal(0, kptn)) != "4")
                                {
                                    String query1 = "SELECT sysmodified FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn1 and isCancelled = 0;";
                                    command.CommandText = query1;
                                    command.Parameters.AddWithValue("kptn1", kptn);
                                }
                                else
                                {
                                    conn.Close();
                                    kplog.Error("FAILED:: respcode: 4, message: Invalid KPTN!");
                                    return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                                }
                                MySqlDataReader dataReader1 = command.ExecuteReader();

                                if (dataReader1.HasRows)
                                {
                                    dataReader1.Read();
                                    // throw new Exception(dataReader1["sysmodified"].GetType());
                                    if (!dataReader1["sysmodified"].GetType().ToString().Equals("System.DBNull"))
                                    {

                                        ClaimDate = Convert.ToDateTime(dataReader1["sysmodified"]);
                                        //TransDate = Convert.ToDateTime(dataReader1["TransDate"]);
                                        dataReader1.Close();
                                    }
                                    else
                                    {
                                        kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(9) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                        dataReader1.Close();
                                        conn.Close();
                                        return new SearchResponse { respcode = 3, message = getRespMessage(9), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                    }
                                }
                                else
                                {
                                    kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                                    dataReader1.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                                }
                            }

                            if ((decodeKPTNGlobal(0, kptn)) != "4")
                            {
                                String query = "SELECT Reason, DormantCharge, Balance, IsRemote, ZoneCode, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SOBranch, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ControlNo, KPTNNo, OperatorID, Principal, Relation, StationID,  IDType, IDNo, ExpiryDate, SenderName, ReceiverName, RemoteBranch, RemoteOperatorID, ControlNo, BranchCode, ClaimedDate, SODate, SOORNo, (SELECT TransPassword FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as TransPass, (SELECT Charge FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Charge, (SELECT OtherCharge FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as OtherCharge, (SELECT Total FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Total, (SELECT Purpose FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Purpose, (SELECT Source FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Source, (SELECT Message FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Message, (SELECT IsClaimed FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as IsClaimed,"
                                    + "(SELECT ControlNo FROM " + decodeKPTNGlobal(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as SendoutControl, remotezonecode,PreferredPO,amountpo,exchangerate FROM " + generateTableNameGlobal(1, ClaimDate.ToString("yyyy-MM-dd HH:mm:ss")) + " WHERE KPTNNo = @kptn ORDER BY ClaimedDate DESC LIMIT 1;";
                                command.CommandText = query;
                                command.Parameters.AddWithValue("kptn", kptn);
                            }
                            else
                            {
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 0, message: INVALID KPTN!");
                                return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                            }

                            MySqlDataReader dataReader = command.ExecuteReader();
                            if (dataReader.HasRows)
                            {
                                dataReader.Read();
                                string sFName = dataReader["SenderFname"].ToString();
                                string sLName = dataReader["SenderLname"].ToString();
                                string sMName = dataReader["SenderMName"].ToString();
                                string sSt = dataReader["SenderStreet"].ToString();
                                string sPCity = dataReader["SenderProvinceCity"].ToString();
                                string sCtry = dataReader["SenderCountry"].ToString();
                                string sG = dataReader["SenderGender"].ToString();
                                string sCNo = dataReader["SenderContactNo"].ToString();
                                Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                                string sBID = dataReader["SOBranch"].ToString();
                                //string sCustID = dataReader["CustID"].ToString();
                                string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                                string SenderName = dataReader["SenderName"].ToString();
                                string rFName = dataReader["ReceiverFname"].ToString();
                                string rLName = dataReader["ReceiverLname"].ToString();
                                string rMName = dataReader["ReceiverMName"].ToString();
                                string rSt = dataReader["ReceiverStreet"].ToString();
                                string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                                string rCtry = dataReader["ReceiverCountry"].ToString();
                                string rG = dataReader["ReceiverGender"].ToString();
                                string rCNo = dataReader["ReceiverContactNo"].ToString();

                                string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                                //string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                                string ReceiverName = dataReader["ReceiverName"].ToString();

                                string SendoutControlNo = dataReader["SendoutControl"].ToString();
                                string KPTNNo = dataReader["KPTNNo"].ToString();
                                //string kptn4 = null;
                                string OperatorID = dataReader["OperatorID"].ToString();
                                //bool IsPassword = (bool)dataReader["IsPassword"];
                                string TransPassword = dataReader["TransPass"].ToString();
                                //DateTime syscreated = (DateTime)dataReader["TransDate "];
                                string Currency = dataReader["PreferredPO"].ToString();
                                Decimal Principal = (Decimal)dataReader["Principal"];
                                //string SenderID = dataReader["CustID"].ToString();
                                Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string Relation = dataReader["Relation"].ToString();
                                string Message = null;
                                string StationID = dataReader["StationID"].ToString();
                                string SourceOfFund = dataReader["Source"].ToString();
                                string IDType = dataReader["IDType"].ToString();
                                string IDNo = dataReader["IDNo"].ToString();
                                string ExpiryDate = dataReader["ExpiryDate"].ToString();
                                string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                                string ControlNo = dataReader["ControlNo"].ToString();
                                string BranchCode = dataReader["BranchCode"].ToString();

                                string ClaimedDate = Convert.ToDateTime(dataReader["ClaimedDate"]).ToString("yyyy-MM-dd HH:mm:ss");
                                string SODate = Convert.ToDateTime(dataReader["SODate"]).ToString("yyyy-MM-dd HH:mm:ss");
                                string SOORNo = dataReader["SOORNo"].ToString();
                                string reason = dataReader["Reason"].ToString();


                                //Decimal Redeem = (Decimal)dataReader["Redeem"];

                                Decimal Total = (Decimal)dataReader["Total"];

                                Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                                Decimal Charge = (Decimal)dataReader["Charge"];
                                string Purpose = dataReader["Purpose"].ToString();
                                string message = dataReader["Message"].ToString();
                                Int32 ZoneCode = Convert.ToInt32(dataReader["ZoneCode"]);
                                //throw new Exception(dataReader["DormantCharge"].GetType().ToString());
                                Decimal Dormant = (dataReader["DormantCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["DormantCharge"];
                                Decimal Balance = (dataReader["Balance"] == DBNull.Value) ? 0 : (Decimal)dataReader["Balance"]; ;
                                Boolean IsRemote = Convert.ToBoolean(dataReader["IsRemote"]);
                                //throw new Exception(dataReader["IsClaimed"].ToString());
                                String IsClaimed = dataReader["IsClaimed"].ToString();
                                Int32 remotezone = Convert.ToInt32(dataReader["remotezonecode"]);
                                String preferredpo = dataReader["preferredpo"].ToString();
                                Decimal amountpo = (Decimal)dataReader["amountpo"];
                                Decimal exchangerate = (Decimal)dataReader["exchangerate"];

                                if (!IsClaimed.Equals("1"))
                                {
                                    kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(9) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                    dataReader.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 3, message = getRespMessage(9), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                }

                                dataReader.Close();
                                //Decimal DormantCharge = CalculateDormantCharge(syscreated);


                                //trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                                command.Transaction = trans;
                                command.Parameters.Clear();
                                command.CommandText = "kpadminlogsglobal.savelog53";
                                command.CommandType = CommandType.StoredProcedure;

                                command.Parameters.AddWithValue("kptnno", KPTNNo);
                                command.Parameters.AddWithValue("action", "PO REPRINT");
                                command.Parameters.AddWithValue("isremote", IsRemote);
                                command.Parameters.AddWithValue("txndate", ClaimedDate);
                                command.Parameters.AddWithValue("stationcode", stationcode);
                                command.Parameters.AddWithValue("stationno", StationID);
                                command.Parameters.AddWithValue("zonecode", ZoneCode);
                                command.Parameters.AddWithValue("branchcode", BranchCode);
                                //command.Parameters.AddWithValue("branchname", DBNull.Value);
                                command.Parameters.AddWithValue("operatorid", OperatorID);
                                command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                                command.Parameters.AddWithValue("remotereason", reason);
                                command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                                command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                                command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                                command.Parameters.AddWithValue("remotezonecode", remotezone);
                                command.Parameters.AddWithValue("type", "N");
                                try
                                {
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCES:: message: stored proc: kpadminlogsglobal.savelog53: "
                                 + "  kptnno: " + KPTNNo
                                 + "  action: " + "PO REPRINT"
                                 + "  isremote: " + IsRemote
                                 + "  txndate: " + ClaimedDate
                                 + "  stationcode: " + stationcode
                                 + "  stationno: " + StationID
                                 + "  zonecode: " + ZoneCode
                                 + "  branchcode: " + BranchCode
                                 + "  operatorid: " + OperatorID
                                 + "  cancelledreason: " + DBNull.Value
                                 + "  remotereason: " + reason
                                 + "  remotebranch: " + DBNull.Value
                                 + "  remoteoperator: " + DBNull.Value
                                 + "  oldkptnno: " + DBNull.Value
                                 + "  remotezonecode: " + remotezone
                                 + "  type: " + "N");
                                    trans.Commit();
                                    conn.Close();
                                }
                                catch (MySqlException ex)
                                {
                                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                                    trans.Rollback();
                                    conn.Close();
                                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                                }
                                si = new SenderInfo
                                {
                                    FirstName = sFName,
                                    LastName = sLName,
                                    MiddleName = sMName,
                                    SenderName = SenderName,
                                    Street = sSt,
                                    ProvinceCity = sPCity,
                                    Country = sCtry,
                                    Gender = sG,
                                    ContactNo = sCNo,
                                    IsSMS = sIsSM,
                                    BranchID = sBID,
                                    //CustID = sCustID,
                                    SenderMLCardNo = sMLCardNo
                                };

                                ri = new ReceiverInfo
                                {
                                    FirstName = rFName,
                                    LastName = rLName,
                                    MiddleName = rMName,
                                    ReceiverName = ReceiverName,
                                    Street = rSt,
                                    ProvinceCity = rPCity,
                                    Country = rCtry,
                                    Gender = rG,
                                    ContactNo = rCNo,
                                    BirthDate = rBdate,
                                    //MLCardNo = rMLCardNo
                                };

                                poi = new PayoutInfo
                                {
                                    SendoutControlNo = SendoutControlNo,
                                    KPTNNo = KPTNNo,
                                    OperatorID = OperatorID,
                                    //IsPassword = IsPassword,
                                    TransPassword = TransPassword,
                                    //syscreated = syscreated,
                                    Currency = Currency,
                                    Principal = Principal,
                                    //SenderID = SenderID,
                                    SenderIsSMS = SenderIsSMS,
                                    Relation = Relation,
                                    Message = Message,
                                    StationID = StationID,
                                    SourceOfFund = SourceOfFund,
                                    //kptn4 = kptn4,
                                    IDNo = IDNo,
                                    IDType = IDType,
                                    ExpiryDate = ExpiryDate,
                                    RemoteBranch = RemoteBranch,
                                    RemoteOperatorID = RemoteOperatorID,
                                    ControlNo = ControlNo,
                                    BranchCode = BranchCode,
                                    ClaimedDate = ClaimedDate,
                                    SODate = SODate,
                                    SOORNo = SOORNo,
                                    Charge = Charge,
                                    OtherCharge = OtherCharge,
                                    Total = Total,
                                    Purpose = Purpose,
                                    SOMessage = message,
                                    ZoneCode = ZoneCode,
                                    IsRemote = IsRemote,
                                    Balance = Balance,
                                    DormantCharge = Dormant,
                                    RemoteZone = remotezone,
                                    prefPO = preferredpo,
                                    amtPO = amountpo,
                                    exchangerate = exchangerate
                                    //DormantCharge = DormantCharge
                                };

                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", PayOutInfo: " + poi);
                                return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, PayoutInfo = poi };

                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                               
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                            }
                        }



                    }
                }
                catch (Exception ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    conn.Close();
                    if (ex.Message.Equals("4"))
                    {
                        kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                        return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                    }
                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
            return new SearchResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
        }
    }


    [WebMethod(BufferResponse = false)]
    public SearchResponse rePrintDomestic(String Username, String Password, String kptn, Int32 type, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptn: " + kptn + ", type: " + type + ", version: " + version + ", stationcode: " + stationcode);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new SearchResponse { respcode = 7, message = getRespMessage(7) };
            }

            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new SearchResponse { respcode = 10, message = getRespMessage(10) };
            //}
            if (type > 1 || 1 < 0)
            {
                kplog.Error("FAILED:: message : " + getRespMessage(0) + ", ErrorDetail : Type must not be greater or less than 1 and 0");
                return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = "Type must not be greater or less than 1 and 0" };
            }
            using (MySqlConnection conn = dbconDomestic.getConnection())
            {
                //DateTime TransDate;
                DateTime ClaimDate = DateTime.Now;
                //Boolean isClaimed;
                try
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    using (command = conn.CreateCommand())
                    {
                        List<object> a = new List<object>();

                        //SerializableDictionary<String, Object> sd = new SerializableDictionary<string, object>();
                        //String query = "SELECT SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, kptn4, IDType, IDNo, ExpiryDate, SenderName, ReceiverName FROM " + generateTableName(2) + " as t INNER JOIN ON " + generateTableName(0) + " s ON t.KPTN6 = s.KPTNNo and t.MLKP4TN = s.kptn4 WHERE (MLKP4TN = @kptn OR MLKP4TN = @kptn) and IsClaimed = 0;";
                        SendoutInfo soi;
                        SenderInfo si;
                        ReceiverInfo ri;
                        PayoutInfo poi;
                        if (type == 0)
                        {
                            //String query = "SELECT SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SenderBranchID, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, IsPassword, TransPassword, TransDate, Currency, Principal, Relation, Message, StationID, Source, kptn4, IDType, IDNo, ExpiryDate, SenderName, ReceiverName, TransDate, RemoteBranch, RemoteOperatorID, ControlNo, BranchCode, Redeem, ORNo FROM " + generateTableName(0, TransDate.ToString("yyyy-MM-dd HH:mm:ss")) + " WHERE (KPTNNo = @kptn OR kptn4 = @kptn);";
                            if ((decodeKPTNDomestic(0, kptn)) != "4")
                            {
                                String query = "SELECT ControlNo, KPTNNo, ORNo, IRNo, OperatorID, StationID, IsRemote, RemoteBranch, RemoteOperatorID, Reason, IsPassword, TransPassword, Purpose, OLDKPTNNo, IsClaimed, IsCancelled, syscreated, syscreator, sysmodified, sysmodifier, Source, Currency, Principal, Charge, OtherCharge, Redeem, Total, Promo, SenderIsSMS, Relation, Message, IDType, IDNo, ExpiryDate, CancelledDate, BranchCode, ZoneCode, TransDate, CancelledByOperatorID, CancelledByBranchCode, CancelledByZoneCode, CancelledByStationID, CancelReason, CancelDetails, CustID, SenderMLCardNo, SenderFName, SenderLName, SenderMName, SenderName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderBirthdate, SenderBranchID, ReceiverMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthDate, CancelCharge, ChargeTo,remotezonecode FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn;";
                                command.CommandText = query;
                                command.Parameters.AddWithValue("kptn", kptn);
                            }
                            else
                            {
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 0 , message: Invalid KPTN");
                                return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                            }
                            MySqlDataReader dataReader = command.ExecuteReader();

                            if (dataReader.HasRows)
                            {
                                dataReader.Read();
                                string sFName = dataReader["SenderFname"].ToString();
                                string sLName = dataReader["SenderLname"].ToString();
                                string sMName = dataReader["SenderMName"].ToString();
                                string sSt = dataReader["SenderStreet"].ToString();
                                string sPCity = dataReader["SenderProvinceCity"].ToString();
                                string sCtry = dataReader["SenderCountry"].ToString();
                                string sG = dataReader["SenderGender"].ToString();
                                string sCNo = dataReader["SenderContactNo"].ToString();
                                Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                                string sBID = dataReader["SenderBranchID"].ToString();
                                string sCustID = dataReader["CustID"].ToString();
                                string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                                string SenderName = dataReader["SenderName"].ToString();

                                string rFName = dataReader["ReceiverFname"].ToString();
                                string rLName = dataReader["ReceiverLname"].ToString();
                                string rMName = dataReader["ReceiverMName"].ToString();
                                string rSt = dataReader["ReceiverStreet"].ToString();
                                string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                                string rCtry = dataReader["ReceiverCountry"].ToString();
                                string rG = dataReader["ReceiverGender"].ToString();
                                string rCNo = dataReader["ReceiverContactNo"].ToString();
                                string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                                string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                                string ReceiverName = dataReader["ReceiverName"].ToString();

                                string SendoutControlNo = dataReader["ControlNo"].ToString();
                                string KPTNNo = dataReader["KPTNNo"].ToString();
                                //string kptn4 = dataReader["kptn4"].ToString();
                                string OperatorID = dataReader["OperatorID"].ToString();
                                bool IsPassword = (bool)dataReader["IsPassword"];
                                string TransPassword = dataReader["TransPassword"].ToString();
                                DateTime syscreated = Convert.ToDateTime(dataReader["TransDate"].ToString());
                                string Currency = dataReader["Currency"].ToString();
                                Decimal Principal = (Decimal)dataReader["Principal"];
                                string SenderID = dataReader["CustID"].ToString();
                                Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string Relation = dataReader["Relation"].ToString();
                                string Message = dataReader["Message"].ToString();
                                string StationID = dataReader["StationID"].ToString();
                                string SourceOfFund = dataReader["Source"].ToString();
                                string IDType = dataReader["IDType"].ToString();
                                string IDNo = dataReader["IDNo"].ToString();
                                string ExpiryDate = dataReader["ExpiryDate"].ToString();
                                //RemoteBranch, RemoteOperatorID, IDType, IDNo, ExpiryDate,ControlNo
                                string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                                string ControlNo = dataReader["ControlNo"].ToString();
                                string BranchCode = dataReader["BranchCode"].ToString();
                                Decimal Redeem = (Decimal)dataReader["Redeem"];
                                string ORNo = dataReader["ORNo"].ToString();
                                Decimal Total = (Decimal)dataReader["Total"];
                                Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                                Decimal Charge = (Decimal)dataReader["Charge"];
                                string Purpose = dataReader["Purpose"].ToString();
                                Int32 ZoneCode = Convert.ToInt32(dataReader["ZoneCode"]);
                                bool IsRemote = (bool)dataReader["IsRemote"];
                                Decimal CancelCharge = (dataReader["CancelCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["CancelCharge"];
                                bool x = (bool)dataReader["IsCancelled"];
                                bool IsClaimed = (bool)dataReader["IsClaimed"];
                                String reason = dataReader["Reason"].ToString();
                                String cancelReason = dataReader["CancelReason"].ToString();
                                Int32 remotezone = Convert.ToInt32(dataReader["remotezonecode"]);

                                // BETA for testing
                                //if (x && !cancelReason.Equals("Return to Sender"))
                                //{
                                //    dataReader.Close();
                                //    conn.Close();
                                //    return new SearchResponse { respcode = 8, message = getRespMessage(8), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                //}

                                //if (!IsClaimed)
                                //{
                                //    dataReader.Close();
                                //    conn.Close();
                                //    return new SearchResponse { respcode = 3, message = getRespMessage(9), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                //}

                                //string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                //string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();

                                dataReader.Close();

                                Decimal DormantCharge = CalculateDormantChargeDomestic(syscreated);


                                command.Transaction = trans;
                                command.Parameters.Clear();
                                command.CommandText = "kpadminlogs.savelog53";
                                command.CommandType = CommandType.StoredProcedure;

                                command.Parameters.AddWithValue("kptnno", KPTNNo);
                                command.Parameters.AddWithValue("action", "SO REPRINT");
                                command.Parameters.AddWithValue("isremote", IsRemote);
                                command.Parameters.AddWithValue("txndate", syscreated);
                                command.Parameters.AddWithValue("stationcode", stationcode);
                                command.Parameters.AddWithValue("stationno", StationID);
                                command.Parameters.AddWithValue("zonecode", ZoneCode);
                                command.Parameters.AddWithValue("branchcode", BranchCode);
                                command.Parameters.AddWithValue("branchname", DBNull.Value);
                                command.Parameters.AddWithValue("operatorid", OperatorID);
                                command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                                command.Parameters.AddWithValue("remotereason", reason);
                                command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                                command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                                command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                                command.Parameters.AddWithValue("remotezonecode", remotezone);
                                command.Parameters.AddWithValue("type", "N");
                                try
                                {
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: respcode: 1 , message: stored proc: kpadminlogs.savelog53: "
                                + " kptnno: " + KPTNNo
                                + " action: " + "SO REPRINT"
                                + " isremote: " + IsRemote
                                + " txndate: " + syscreated
                                + " stationcode: " + stationcode
                                + " stationno: " + StationID
                                + " zonecode: " + ZoneCode
                                + " branchcode: " + BranchCode
                                + " branchname: " + DBNull.Value
                                + " operatorid: " + OperatorID
                                + " cancelledreason: " + DBNull.Value
                                + " remotereason: " + reason
                                + " remotebranch: " + DBNull.Value
                                + " remoteoperator: " + DBNull.Value
                                + " oldkptnno: " + DBNull.Value
                                + " remotezonecode: " + remotezone
                                + " type: " + "N");
                                    trans.Commit();
                                    conn.Close();
                                }
                                catch (MySqlException ex)
                                {
                                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                                    trans.Rollback();
                                    conn.Close();
                                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                                }

                                si = new SenderInfo
                                {
                                    FirstName = sFName,
                                    LastName = sLName,
                                    MiddleName = sMName,
                                    SenderName = SenderName,
                                    Street = sSt,
                                    ProvinceCity = sPCity,
                                    Country = sCtry,
                                    Gender = sG,
                                    ContactNo = sCNo,
                                    IsSMS = sIsSM,
                                    BranchID = sBID,
                                    CustID = sCustID,
                                    SenderMLCardNo = sMLCardNo
                                };

                                ri = new ReceiverInfo
                                {
                                    FirstName = rFName,
                                    LastName = rLName,
                                    MiddleName = rMName,
                                    ReceiverName = ReceiverName,
                                    Street = rSt,
                                    ProvinceCity = rPCity,
                                    Country = rCtry,
                                    Gender = rG,
                                    ContactNo = rCNo,
                                    BirthDate = rBdate,
                                    MLCardNo = rMLCardNo
                                };

                                soi = new SendoutInfo
                                {
                                    SendoutControlNo = SendoutControlNo,
                                    KPTNNo = KPTNNo,
                                    OperatorID = OperatorID,
                                    IsPassword = IsPassword,
                                    TransPassword = TransPassword,
                                    syscreated = syscreated,
                                    Currency = Currency,
                                    Principal = Principal,
                                    SenderID = SenderID,
                                    SenderIsSMS = SenderIsSMS,
                                    Relation = Relation,
                                    Message = Message,
                                    StationID = StationID,
                                    SourceOfFund = SourceOfFund,
                                    //kptn4 = kptn4,
                                    IDNo = IDNo,
                                    IDType = IDType,
                                    ExpiryDate = ExpiryDate,
                                    DormantCharge = DormantCharge,
                                    RemoteOperatorID = RemoteOperatorID,
                                    RemoteBranch = RemoteBranch,
                                    BranchCode = BranchCode,
                                    Redeem = Redeem,
                                    ORNo = ORNo,
                                    Charge = Charge,
                                    OtherCharge = OtherCharge,
                                    Purpose = Purpose,
                                    Total = Total,
                                    ZoneCode = ZoneCode,
                                    IsRemote = IsRemote,
                                    CancelCharge = CancelCharge,
                                    RemoteReason = reason,
                                    RemoteZone = remotezone
                                };
                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo : " + si + ", RecieverInfo: " + ri + ", SendoutInfo: " + soi);
                                return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, SendoutInfo = soi };
                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                            }
                        }
                        else
                        {
                            using (command = conn.CreateCommand())
                            {
                                if ((decodeKPTNDomestic(0, kptn)) != "4")
                                {
                                    String query1 = "SELECT sysmodified FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn1 and isCancelled = 0;";
                                    command.CommandText = query1;
                                    command.Parameters.AddWithValue("kptn1", kptn);
                                }
                                else
                                {
                                    conn.Open();
                                    kplog.Error("FAILED:: respcode: 0 , message: Invalid KPTN!");
                                    return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                                }
                                MySqlDataReader dataReader1 = command.ExecuteReader();

                                if (dataReader1.HasRows)
                                {
                                    dataReader1.Read();
                                    // throw new Exception(dataReader1["sysmodified"].GetType());
                                    if (!dataReader1["sysmodified"].GetType().ToString().Equals("System.DBNull"))
                                    {

                                        ClaimDate = Convert.ToDateTime(dataReader1["sysmodified"]);
                                        //TransDate = Convert.ToDateTime(dataReader1["TransDate"]);
                                        dataReader1.Close();
                                    }
                                    else
                                    {
                                        kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(9) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                        dataReader1.Close();
                                        conn.Close();
                                        return new SearchResponse { respcode = 3, message = getRespMessage(9), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                    }
                                }
                                else
                                {
                                    kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                                    dataReader1.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                                }
                            }

                            if ((decodeKPTNDomestic(0, kptn)) != "4")
                            {
                                String query = "SELECT Reason, DormantCharge, Balance, IsRemote, ZoneCode, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, SenderIsSMS,SenderBirthdate, SOBranch, CustID, SenderMLCardNo, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, ReceiverMLCardNo, ControlNo, KPTNNo, OperatorID, Currency, Principal, Relation, StationID,  IDType, IDNo, ExpiryDate, SenderName, ReceiverName, RemoteBranch, RemoteOperatorID, ControlNo, BranchCode, ClaimedDate, SODate, SOORNo, (SELECT TransPassword FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as TransPass, (SELECT Charge FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Charge, (SELECT OtherCharge FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as OtherCharge, (SELECT Total FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Total, (SELECT Purpose FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Purpose, (SELECT Source FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Source, (SELECT Message FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as Message, (SELECT IsClaimed FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as IsClaimed,"
                                    + "(SELECT ControlNo FROM " + decodeKPTNDomestic(0, kptn) + " WHERE KPTNNo = @kptn and isCancelled = 0) as SendoutControl, remotezonecode FROM " + generateTableNameDomestic(1, ClaimDate.ToString("yyyy-MM-dd HH:mm:ss")) + " WHERE KPTNNo = @kptn ORDER BY ClaimedDate DESC LIMIT 1;";
                                command.CommandText = query;
                                command.Parameters.AddWithValue("kptn", kptn);
                            }
                            else
                            {
                                conn.Close();
                                kplog.Error("FAILED:: respcode: 0 , message: Invalid KPTN");
                                return new SearchResponse { respcode = 0, message = "Invalid KPTN" };
                            }

                            MySqlDataReader dataReader = command.ExecuteReader();
                            if (dataReader.HasRows)
                            {
                                dataReader.Read();
                                string sFName = dataReader["SenderFname"].ToString();
                                string sLName = dataReader["SenderLname"].ToString();
                                string sMName = dataReader["SenderMName"].ToString();
                                string sSt = dataReader["SenderStreet"].ToString();
                                string sPCity = dataReader["SenderProvinceCity"].ToString();
                                string sCtry = dataReader["SenderCountry"].ToString();
                                string sG = dataReader["SenderGender"].ToString();
                                string sCNo = dataReader["SenderContactNo"].ToString();
                                Int32 sIsSM = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string sBdate = (dataReader["SenderBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["SenderBirthdate"].ToString();
                                string sBID = dataReader["SOBranch"].ToString();
                                string sCustID = dataReader["CustID"].ToString();
                                string sMLCardNo = dataReader["SenderMLCardNo"].ToString();
                                string SenderName = dataReader["SenderName"].ToString();
                                string rFName = dataReader["ReceiverFname"].ToString();
                                string rLName = dataReader["ReceiverLname"].ToString();
                                string rMName = dataReader["ReceiverMName"].ToString();
                                string rSt = dataReader["ReceiverStreet"].ToString();
                                string rPCity = dataReader["ReceiverProvinceCity"].ToString();
                                string rCtry = dataReader["ReceiverCountry"].ToString();
                                string rG = dataReader["ReceiverGender"].ToString();
                                string rCNo = dataReader["ReceiverContactNo"].ToString();

                                string rBdate = (dataReader["ReceiverBirthdate"].ToString().Equals("0/0/0000")) ? String.Empty : dataReader["ReceiverBirthdate"].ToString();

                                string rMLCardNo = dataReader["ReceiverMLCardNo"].ToString();
                                string ReceiverName = dataReader["ReceiverName"].ToString();

                                string SendoutControlNo = dataReader["SendoutControl"].ToString();
                                string KPTNNo = dataReader["KPTNNo"].ToString();
                                //string kptn4 = null;
                                string OperatorID = dataReader["OperatorID"].ToString();
                                //bool IsPassword = (bool)dataReader["IsPassword"];
                                string TransPassword = dataReader["TransPass"].ToString();
                                //DateTime syscreated = (DateTime)dataReader["TransDate "];
                                string Currency = dataReader["Currency"].ToString();
                                Decimal Principal = (Decimal)dataReader["Principal"];
                                string SenderID = dataReader["CustID"].ToString();
                                Int32 SenderIsSMS = Convert.ToInt32(dataReader["SenderIsSMS"]);
                                string Relation = dataReader["Relation"].ToString();
                                string Message = null;
                                string StationID = dataReader["StationID"].ToString();
                                string SourceOfFund = dataReader["Source"].ToString();
                                string IDType = dataReader["IDType"].ToString();
                                string IDNo = dataReader["IDNo"].ToString();
                                string ExpiryDate = dataReader["ExpiryDate"].ToString();
                                string RemoteBranch = dataReader["RemoteBranch"].ToString();
                                string RemoteOperatorID = dataReader["RemoteOperatorID"].ToString();
                                string ControlNo = dataReader["ControlNo"].ToString();
                                string BranchCode = dataReader["BranchCode"].ToString();

                                string ClaimedDate = Convert.ToDateTime(dataReader["ClaimedDate"]).ToString("yyyy-MM-dd HH:mm:ss");
                                string SODate = Convert.ToDateTime(dataReader["SODate"]).ToString("yyyy-MM-dd HH:mm:ss");
                                string SOORNo = dataReader["SOORNo"].ToString();
                                string reason = dataReader["Reason"].ToString();


                                //Decimal Redeem = (Decimal)dataReader["Redeem"];

                                Decimal Total = (Decimal)dataReader["Total"];

                                Decimal OtherCharge = (Decimal)dataReader["OtherCharge"];
                                Decimal Charge = (Decimal)dataReader["Charge"];
                                string Purpose = dataReader["Purpose"].ToString();
                                string message = dataReader["Message"].ToString();
                                Int32 ZoneCode = Convert.ToInt32(dataReader["ZoneCode"]);
                                //throw new Exception(dataReader["DormantCharge"].GetType().ToString());
                                Decimal Dormant = (dataReader["DormantCharge"] == DBNull.Value) ? 0 : (Decimal)dataReader["DormantCharge"];
                                Decimal Balance = (dataReader["Balance"] == DBNull.Value) ? 0 : (Decimal)dataReader["Balance"]; ;
                                bool IsRemote = (bool)dataReader["IsRemote"];
                                //throw new Exception(dataReader["IsClaimed"].ToString());
                                String IsClaimed = dataReader["IsClaimed"].ToString();
                                Int32 remotezone = Convert.ToInt32(dataReader["remotezonecode"]);

                                if (!IsClaimed.Equals("1"))
                                {
                                    kplog.Error("FAILED:: respcode: 3, message: " + getRespMessage(9) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                                    dataReader.Close();
                                    conn.Close();
                                    return new SearchResponse { respcode = 3, message = getRespMessage(9), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                                }

                                dataReader.Close();
                                //Decimal DormantCharge = CalculateDormantCharge(syscreated);


                                //trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                                command.Transaction = trans;
                                command.Parameters.Clear();
                                command.CommandText = "kpadminlogs.savelog53";
                                command.CommandType = CommandType.StoredProcedure;

                                command.Parameters.AddWithValue("kptnno", KPTNNo);
                                command.Parameters.AddWithValue("action", "PO REPRINT");
                                command.Parameters.AddWithValue("isremote", IsRemote);
                                command.Parameters.AddWithValue("txndate", ClaimedDate);
                                command.Parameters.AddWithValue("stationcode", stationcode);
                                command.Parameters.AddWithValue("stationno", StationID);
                                command.Parameters.AddWithValue("zonecode", ZoneCode);
                                command.Parameters.AddWithValue("branchcode", BranchCode);
                                command.Parameters.AddWithValue("branchname", DBNull.Value);
                                command.Parameters.AddWithValue("operatorid", OperatorID);
                                command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                                command.Parameters.AddWithValue("remotereason", reason);
                                command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value)) ? null : RemoteBranch);
                                command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                                command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                                command.Parameters.AddWithValue("remotezonecode", remotezone);
                                command.Parameters.AddWithValue("type", "N");
                                try
                                {
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: stored proc: kpadminlogs.savelog53: "
                                + " kptnno: " + KPTNNo
                                + " action: " + "PO REPRINT"
                                + " isremote: " + IsRemote
                                + " txndate: " + ClaimedDate
                                + " stationcode: " + stationcode
                                + " stationno: " + StationID
                                + " zonecode: " + ZoneCode
                                + " branchcode: " + BranchCode
                                + " branchname: " + DBNull.Value
                                + " operatorid: " + OperatorID
                                + " cancelledreason: " + DBNull.Value
                                + " remotereason: " + reason
                                + " remotebranch: " + DBNull.Value
                                + " remoteoperator: " + DBNull.Value
                                + " oldkptnno: " + DBNull.Value
                                + " remotezonecode: " + remotezone
                                + " type: " + "N");
                                    trans.Commit();
                                    conn.Close();
                                }
                                catch (MySqlException ex)
                                {
                                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                                    trans.Rollback();
                                    conn.Close();
                                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                                }
                                si = new SenderInfo
                                {
                                    FirstName = sFName,
                                    LastName = sLName,
                                    MiddleName = sMName,
                                    SenderName = SenderName,
                                    Street = sSt,
                                    ProvinceCity = sPCity,
                                    Country = sCtry,
                                    Gender = sG,
                                    ContactNo = sCNo,
                                    IsSMS = sIsSM,
                                    BranchID = sBID,
                                    CustID = sCustID,
                                    SenderMLCardNo = sMLCardNo
                                };

                                ri = new ReceiverInfo
                                {
                                    FirstName = rFName,
                                    LastName = rLName,
                                    MiddleName = rMName,
                                    ReceiverName = ReceiverName,
                                    Street = rSt,
                                    ProvinceCity = rPCity,
                                    Country = rCtry,
                                    Gender = rG,
                                    ContactNo = rCNo,
                                    BirthDate = rBdate,
                                    MLCardNo = rMLCardNo
                                };

                                poi = new PayoutInfo
                                {
                                    SendoutControlNo = SendoutControlNo,
                                    KPTNNo = KPTNNo,
                                    OperatorID = OperatorID,
                                    //IsPassword = IsPassword,
                                    TransPassword = TransPassword,
                                    //syscreated = syscreated,
                                    Currency = Currency,
                                    Principal = Principal,
                                    SenderID = SenderID,
                                    SenderIsSMS = SenderIsSMS,
                                    Relation = Relation,
                                    Message = Message,
                                    StationID = StationID,
                                    SourceOfFund = SourceOfFund,
                                    //kptn4 = kptn4,
                                    IDNo = IDNo,
                                    IDType = IDType,
                                    ExpiryDate = ExpiryDate,
                                    RemoteBranch = RemoteBranch,
                                    RemoteOperatorID = RemoteOperatorID,
                                    ControlNo = ControlNo,
                                    BranchCode = BranchCode,
                                    ClaimedDate = ClaimedDate,
                                    SODate = SODate,
                                    SOORNo = SOORNo,
                                    Charge = Charge,
                                    OtherCharge = OtherCharge,
                                    Total = Total,
                                    Purpose = Purpose,
                                    SOMessage = message,
                                    ZoneCode = ZoneCode,
                                    IsRemote = IsRemote,
                                    Balance = Balance,
                                    DormantCharge = Dormant,
                                    RemoteZone = remotezone
                                    //DormantCharge = DormantCharge
                                };

                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", SenderInfo: " + si + ", RecieverInfo: " + ri + ", PayOutInfo: " + poi);
                                return new SearchResponse { respcode = 1, message = getRespMessage(1), SenderInfo = si, ReceiverInfo = ri, PayoutInfo = poi };

                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4));
                                dataReader.Close();
                                conn.Close();
                                return new SearchResponse { respcode = 4, message = getRespMessage(4) };
                            }
                        }



                    }
                }
                catch (Exception ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
                    conn.Close();
                    if (ex.Message.Equals("4"))
                    {
                        kplog.Error("FAILED:: respcode: 4, message: " + getRespMessage(4) + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null); ;
                        return new SearchResponse { respcode = 4, message = getRespMessage(4), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                    }
                    return new SearchResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + ex.ToString() + ", SenderInfo" + null + ", RecieverInfo:  " + null + ", SendoutInfo: " + null);
            return new SearchResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString(), SenderInfo = null, ReceiverInfo = null, SendoutInfo = null };
        }
    }




    [WebMethod]
    public KYCResponse retrieveCustomer(string mlcardno)
    {
        kplog.Info("mlcardno: "+mlcardno);

        string retrieve = "select * from kpcustomersglobal.customer where MLCardNo = @cardno";
        string lastname = null;
        string firstname = null;
        string middlename = null;
        string primary_street = null;
        string primary_province = null;
        string primary_country = null;
        string birthdate = null;
        string gender = null;
        string mobileno = null;
        string id1_idno = null;
        string id1_expirydate = null;
        string id1_idtype = null;
        //string id1_idtype;
        using (MySqlConnection conn = dbconGlobal.getConnection())
        {
            try
            {
                conn.Open();
                MySqlCommand cmd = new MySqlCommand(retrieve, conn);
                MySqlDataReader dataReaderretrieve = cmd.ExecuteReader();
                if (dataReaderretrieve.Read())
                {
                    if ((int)dataReaderretrieve["state"] == 1)
                    {
                        string retrieveCustomer = "select * from mlkyc.customer where custno = " + dataReaderretrieve["custid"] + "";
                        dataReaderretrieve.Close();
                        MySqlCommand cmdcustomer = new MySqlCommand(retrieveCustomer, dbconGlobal.getConnection());
                        MySqlDataReader dataReadercustomer = cmdcustomer.ExecuteReader();
                        while (dataReadercustomer.Read())
                        {
                            lastname = dataReadercustomer["lastname"].ToString();
                            firstname = dataReadercustomer["firstname"].ToString();
                            middlename = dataReadercustomer["middlename"].ToString();
                            primary_street = dataReadercustomer["primary_street"].ToString();
                            primary_province = dataReadercustomer["primary_province"].ToString();
                            primary_country = dataReadercustomer["primary_country"].ToString();
                            birthdate = dataReadercustomer["birthdate"].ToString();
                            gender = dataReadercustomer["gender"].ToString();
                            mobileno = dataReadercustomer["mobileno"].ToString();
                            id1_idno = dataReadercustomer["id1_idno"].ToString();
                            id1_expirydate = dataReadercustomer["id1_expirydate"].ToString();
                            id1_idtype = dataReadercustomer["id1_idtype"].ToString();
                        }
                        //throw new Exception(lastname);
                        dbconGlobal.CloseConnection();
                        kplog.Info("SUCCESS:: respcode: 1 , message : " + lastname + " " + firstname + " " + middlename + " " + primary_street + " " + primary_province + " " + primary_country + " " + birthdate + " " + gender + " " + mobileno + " " + id1_idno + " " + id1_expirydate + " " + id1_idtype);
                        return new KYCResponse(1, "SUCCESS", lastname, firstname, middlename, primary_street, primary_province, primary_country, birthdate, gender, mobileno, id1_idno, id1_expirydate, id1_idtype);
                    }

                }
                else
                {
                    dbconGlobal.CloseConnection();
                    kplog.Error("FAILED:: respcode: 0 , message: MLCard No. Not Found!");
                    return new KYCResponse(0, "FAILED", "MLCARD NO NOT FOUND");
                }

            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                return new KYCResponse(0, "FAILED", ex.ToString());
            }
        }
        return null;
    }

    [WebMethod]
    public ControlResponse generateControlGlobalMobile(String Username, String Password, String branchcode, Int32 type, String OperatorID, Int32 ZoneCode, String StationNumber, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", branchcode: " + branchcode + ", type: " + type + ", OperatorID: " + OperatorID + ", ZoneCode: " + ZoneCode + ", StationNumber: " + StationNumber + ", version: " + version + ", stationcode: " + stationcode);

        if (StationNumber.ToString().Equals("0"))
        {
            kplog.Error("FAILED:: respcode: 13 , message: " + getRespMessage(13));
            return new ControlResponse { respcode = 13, message = getRespMessage(13) };
        }
        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ControlResponse { respcode = 7, message = getRespMessage(7) };
        }
        try
        {
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    command.Transaction = trans;
                    try
                    {
                        dt = getServerDateGlobal(true);
                        String control = "MLG";
                        String getcontrolmax = string.Empty;
                        Int32 xres = 0;

                        command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpformsglobal.control where station = @st and bcode = @bcode and zcode = @zcode and `type` = @tp FOR UPDATE";
                        command.Parameters.AddWithValue("st", StationNumber);
                        command.Parameters.AddWithValue("bcode", branchcode);
                        command.Parameters.AddWithValue("zcode", ZoneCode);
                        command.Parameters.AddWithValue("tp", type);
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.HasRows)
                        {

                            Reader.Read();

                            if (type == 0)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 1)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 2)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else if (type == 3)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else
                            {
                                kplog.Error("FAILED:: respcode: 0 , message : Invalid type value");
                                throw new Exception("Invalid type value");
                            }

                            String s = Reader["Station"].ToString();
                            String nseries = Reader["nseries"].ToString().PadLeft(5, '0');
                            Reader.Close();

                            trans.Commit();
                            conn.Close();
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + "M" + nseries) + ", nseries: " + nseries);
                            return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "M" + nseries, nseries = nseries };

                        }
                        else
                        {
                            Reader.Close();
                            command.CommandText = "Insert into kpformsglobal.control (`station`,`bcode`,`userid`,`nseries`,`zcode`, `type`) values (@station,@branchcode,@uid,1,@zonecode,@type)";
                            if (type == 0)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 1)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 2)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else if (type == 3)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else
                            {
                                kplog.Error("FAILED:: message : Invalid type value");
                                throw new Exception("Invalid type value");
                            }
                            command.Parameters.AddWithValue("station", StationNumber);
                            command.Parameters.AddWithValue("branchcode", branchcode);
                            command.Parameters.AddWithValue("uid", OperatorID);
                            command.Parameters.AddWithValue("zonecode", ZoneCode);
                            command.Parameters.AddWithValue("type", type);
                            int x = command.ExecuteNonQuery();

                            trans.Commit();
                            conn.Close();
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + "M 00001") + ", nseries: 00001");
                            return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "M" + "00001", nseries = "00001" };
                        }
                    }
                    catch (MySqlException ex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                        return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconGlobal.CloseConnection();
            return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }

        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconGlobal.CloseConnection();
            return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public ControlResponse generateControlGlobal(String Username, String Password, String branchcode, Int32 type, String OperatorID, Int32 ZoneCode, String StationNumber, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", branchcode: " + branchcode + ", type: " + type + ", OperatorID: " + OperatorID + ", ZoneCode: " + ZoneCode + ", StationNumber: " + StationNumber + ", version: " + version + ", stationcode: " + stationcode);

        if (StationNumber.ToString().Equals("0"))
        {
            kplog.Error("FAILED:: message: " + getRespMessage(13));
            return new ControlResponse { respcode = 13, message = getRespMessage(13) };
        }
        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ControlResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ControlResponse { respcode = 10, message = getRespMessage(10) };
        //}
        try
        {
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    command.Transaction = trans;
                    try
                    {
                        dt = getServerDateGlobal(true);
                        String control = "MLG";
                        String getcontrolmax = string.Empty;
                        Int32 xres = 0;

                        //command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpformsglobal.control where station = @st and bcode = @bcode and zcode = @zcode and type = @tp FOR UPDATE";
                        command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpformsglobal.control where station = @st and bcode = @bcode and zcode = @zcode and `type` = @tp FOR UPDATE";
                        command.Parameters.AddWithValue("st", StationNumber);
                        command.Parameters.AddWithValue("bcode", branchcode);
                        command.Parameters.AddWithValue("zcode", ZoneCode);
                        command.Parameters.AddWithValue("tp", type);
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.HasRows)
                        {
                            //throw new Exception("Invalid type value");
                            Reader.Read();
                            //throw new Exception(Reader["station"].ToString() + " " + Reader["bcode"].ToString() + " " + Reader["type"].ToString());
                            if (type == 0)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 1)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 2)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else if (type == 3)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else
                            {
                                kplog.Error("FAILED:: message: Invalid type value");
                                throw new Exception("Invalid type value");
                            }

                            String s = Reader["Station"].ToString();
                            String nseries = Reader["nseries"].ToString().PadLeft(6, '0');
                            Reader.Close();

                            //if (type == 0)
                            //{ getcontrolmax = "select kptnno from kpadminlogsglobal.transactionlogs where branchcode = @branchcode and stationno = @stationid and zonecode = @zonecode and DATE_FORMAT(txndate,'%Y') = @trxndate and action = 'SENDOUT' limit 1"; }
                            //else if (type == 1)
                            //{ getcontrolmax = "select kptnno from kpadminlogsglobal.transactionlogs where branchcode = @branchcode and stationno = @stationid and zonecode = @zonecode and DATE_FORMAT(txndate,'%Y') = @trxndate and action = 'PAYOUT' limit 1"; }
                            //else if (type == 2)
                            //{ getcontrolmax = "select kptnno from kpadminlogsglobal.transactionlogs where remotebranch = @branchcode and remotezonecode = @zonecode and DATE_FORMAT(txndate,'%Y') = @trxndate and action = 'SENDOUT' and IsRemote = 1 limit 1"; }
                            //else
                            //{ getcontrolmax = "select kptnno from kpadminlogsglobal.transactionlogs where remotebranch = @branchcode and remotezonecode = @zonecode and DATE_FORMAT(txndate,'%Y') = @trxndate and action = 'PAYOUT' and IsRemote = 1 limit 1"; }

                            //command.CommandText = getcontrolmax;
                            //command.Parameters.Clear();
                            //command.Parameters.AddWithValue("branchcode", branchcode);
                            //command.Parameters.AddWithValue("stationid", StationNumber);
                            //command.Parameters.AddWithValue("zonecode", ZoneCode);
                            //command.Parameters.AddWithValue("trxndate", dt.ToString("yyyy"));
                            ////String dd = dt.ToString("yyyy-MM-dd");
                            //MySqlDataReader rdrcontrol = command.ExecuteReader();
                            //if (rdrcontrol.Read())
                            //{
                            //    xres = 1;
                            //}
                            //rdrcontrol.Close();
                            trans.Commit();
                            conn.Close();

                            //if (isSameYear2(dt))
                            //{
                            //    if (xres == 0)
                            //    {
                            //        return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "000001", nseries = "000001" };
                            //    }
                            //    else
                            //    {
                            //        return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + nseries, nseries = nseries };
                            //    }
                            //}
                            //else
                            //{
                            //    if (xres == 0)
                            //    {
                            //        return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "000001", nseries = "000001" };
                            //    }
                            //    else
                            //    {
                            //        return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + nseries, nseries = nseries };
                            //    }
                            //}
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + nseries) + ", nseries: " + nseries);
                            return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + nseries, nseries = nseries };

                        }
                        else
                        {
                            Reader.Close();
                            command.CommandText = "Insert into kpformsglobal.control (`station`,`bcode`,`userid`,`nseries`,`zcode`, `type`) values (@station,@branchcode,@uid,1,@zonecode,@type)";
                            if (type == 0)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 1)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 2)
                            {
                                control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else if (type == 3)
                            {
                                control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else
                            {
                                kplog.Error("FAILED:: message: Invalid type value");
                                throw new Exception("Invalid type value");
                            }
                            command.Parameters.AddWithValue("station", StationNumber);
                            command.Parameters.AddWithValue("branchcode", branchcode);
                            command.Parameters.AddWithValue("uid", OperatorID);
                            command.Parameters.AddWithValue("zonecode", ZoneCode);
                            command.Parameters.AddWithValue("type", type);
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: Insert into kpformsglobal.control: "
                           + " station: " + StationNumber
                           + " branchcode: " + branchcode
                           + " uid: " + OperatorID
                           + " zonecode: " + ZoneCode
                           + " type: " + type);
                            //if (x < 1) {
                            //    conn.Close();
                            //    throw new Exception("asdfsadfds");
                            //}
                            trans.Commit();
                            conn.Close();
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + "000001") + ", nseries: 000001");
                            return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "000001", nseries = "000001" };
                        }
                    }
                    catch (MySqlException ex)
                    {
                        trans.Rollback();
                        conn.Close();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                        return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconGlobal.CloseConnection();
            return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }

        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconGlobal.CloseConnection();
            return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }


    [WebMethod]
    public ControlResponse generateControlDomestic(String Username, String Password, String branchcode, Int32 type, String OperatorID, Int32 ZoneCode, String StationNumber, Double version, String stationcode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", branchcode: " + branchcode + ", type: " + type + ", OperatorID: " + OperatorID + ", ZoneCode: " + ZoneCode + ", StationNumber: " + StationNumber + ", version: " + version + ", stationcode: " + stationcode);

        if (StationNumber.ToString().Equals("0"))
        {
            kplog.Error("FAILED:: message: " + getRespMessage(13));
            return new ControlResponse { respcode = 13, message = getRespMessage(13) };
        }
        if (!authenticate(Username, Password))
        {

            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ControlResponse { respcode = 7, message = getRespMessage(7) };
        }
        //if (!compareVersions(getVersion(stationcode), version))
        //{
        //    return new ControlResponse { respcode = 10, message = getRespMessage(10) };
        //}
        try
        {
            using (MySqlConnection conn = dbconDomestic.getConnection())
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    command.Transaction = trans;
                    try
                    {
                        dt = getServerDateDomestic(true);
                        String control;

                        //command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpformsglobal.control where station = @st and bcode = @bcode and zcode = @zcode and type = @tp FOR UPDATE";
                        command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpforms.control where station = @st and bcode = @bcode and zcode = @zcode and `type` = @tp FOR UPDATE";
                        command.Parameters.AddWithValue("st", StationNumber);
                        command.Parameters.AddWithValue("bcode", branchcode);
                        command.Parameters.AddWithValue("zcode", ZoneCode);
                        command.Parameters.AddWithValue("tp", type);
                        MySqlDataReader Reader = command.ExecuteReader();

                        if (Reader.HasRows)
                        {
                            //throw new Exception("Invalid type value");
                            Reader.Read();
                            //throw new Exception(Reader["station"].ToString() + " " + Reader["bcode"].ToString() + " " + Reader["type"].ToString());
                            if (type == 0)
                            {
                                control = "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 1)
                            {
                                control = "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 2)
                            {
                                control = "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else if (type == 3)
                            {
                                control = "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else
                            {
                                kplog.Error("FAILED:: message: Invalid type value");
                                throw new Exception("Invalid type value");
                            }

                            String s = Reader["Station"].ToString();
                            String nseries = Reader["nseries"].ToString().PadLeft(6, '0');
                            Reader.Close();
                            trans.Commit();
                            conn.Close();
                            if (isSameYear2(dt))
                            {
                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + nseries) + ", nseries: " + nseries);
                                return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + nseries, nseries = nseries };
                            }
                            else
                            {
                                kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + "000001") + ", nseries: 000001");
                                return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "000001", nseries = "000001" };
                            }

                        }
                        else
                        {
                            Reader.Close();
                            command.CommandText = "Insert into kpforms.control (`station`,`bcode`,`userid`,`nseries`,`zcode`, `type`) values (@station,@branchcode,@uid,1,@zonecode,@type)";
                            if (type == 0)
                            {
                                control = "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 1)
                            {
                                control = "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                            }
                            else if (type == 2)
                            {
                                control = "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else if (type == 3)
                            {
                                control = "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                            }
                            else
                            {
                                kplog.Error("FAILED:: message: Invalid type value");
                                throw new Exception("Invalid type value");
                            }
                            command.Parameters.AddWithValue("station", StationNumber);
                            command.Parameters.AddWithValue("branchcode", branchcode);
                            command.Parameters.AddWithValue("uid", OperatorID);
                            command.Parameters.AddWithValue("zonecode", ZoneCode);
                            command.Parameters.AddWithValue("type", type);
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: Insert into kpforms.control : "
                            + " station: " + StationNumber
                            + " branchcode: " + branchcode
                            + " uid: " + OperatorID
                            + " zonecode: " + ZoneCode
                            + " type: " + type);
                            //if (x < 1) {
                            //    conn.Close();
                            //    throw new Exception("asdfsadfds");
                            //}
                            trans.Commit();
                            conn.Close();
                            kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", controlno: " + (control + "-" + dt.ToString("yy") + "-" + "000001") + ", nseries: 000001");
                            return new ControlResponse { respcode = 1, message = getRespMessage(1), controlno = control + "-" + dt.ToString("yy") + "-" + "000001", nseries = "000001" };
                        }
                    }
                    catch (MySqlException ex)
                    {
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                        trans.Rollback();
                        conn.Close();
                        return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconDomestic.CloseConnection();
            return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }

        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconDomestic.CloseConnection();
            return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public String generateORNOGlobal(string branchcode, string zonecode, Double version, String stationcode)
    {
        kplog.Info("branchcode: "+branchcode+", zonecode: "+zonecode+", version: "+version+", stationcode: "+stationcode);
        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                conn.Open();
                trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                try
                {
                    using (command = conn.CreateCommand())
                    {
                        command.Transaction = trans;
                        dt = getServerDateGlobal(true);
                        string query = "select oryear,branchcode,zonecode,series from kpformsglobal.resibo where branchcode = @bcode1 and zonecode = @zcode1 FOR UPDATE";
                        command.CommandText = query;
                        command.Parameters.AddWithValue("bcode1", branchcode);
                        command.Parameters.AddWithValue("zcode1", zonecode);
                        //trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                        using (MySqlDataReader dataReader = command.ExecuteReader())
                        {
                            if (dataReader.HasRows)
                            {

                                dataReader.Read();
                                Int32 series = Convert.ToInt32(dataReader["series"]) + 1;
                                String oryear = dataReader["oryear"].ToString().Substring(2);
                                dataReader.Close();
                                //if (isSameYear2(dt))
                                //{
                                //    command.Parameters.Clear();
                                //    command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                                //    command.Parameters.AddWithValue("bcode2", branchcode);
                                //    command.Parameters.AddWithValue("zcode2", zonecode);
                                //    command.Parameters.AddWithValue("series", series);
                                //    command.ExecuteNonQuery();
                                //    trans.Commit();
                                //    conn.Close();
                                //    return oryear + "-" + series.ToString().PadLeft(6, '0');
                                //}
                                //else
                                //{
                                //    kplog.Info("SERIES RESETTED");
                                //    command.Parameters.Clear();
                                //    command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                                //    command.Parameters.AddWithValue("bcode2", branchcode);
                                //    command.Parameters.AddWithValue("zcode2", zonecode);
                                //    command.Parameters.AddWithValue("series", 1);
                                //    command.ExecuteNonQuery();
                                //    trans.Commit();
                                //    conn.Close();
                                //    return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');
                                //}

                                return oryear + "-" + series.ToString().PadLeft(6, '0');

                            }
                            else
                            {
                                dataReader.Close();
                                //String oryear = dataReader["oryear"].ToString().Substring(2);
                                //dataReader.Close();
                                command.Parameters.Clear();
                                command.CommandText = "update kpformsglobal.resibo set `lock` = 1 where branchcode = @bcode2 and zonecode = @zcode2";
                                command.Parameters.AddWithValue("bcode2", branchcode);
                                command.Parameters.AddWithValue("zcode2", zonecode);
                                command.ExecuteNonQuery();
                                kplog.Info("SUCCES:: message: update kpformsglobal.resibo: lock : 1, branchcode: " + branchcode + ", zonecode: " + zonecode);

                                command.Parameters.Clear();
                                command.CommandText = "insert into kpformsglobal.resibo (oryear, branchcode, zonecode, series) values (@year, @bcode2, @zcode2, @ser)";
                                command.Parameters.AddWithValue("year", dt.ToString("yyyy"));
                                command.Parameters.AddWithValue("bcode2", branchcode);
                                command.Parameters.AddWithValue("zcode2", zonecode);
                                command.Parameters.AddWithValue("ser", 1);
                                command.ExecuteNonQuery();
                                kplog.Info("SUCCESS:: message: insert into kpformsglobal.resibo: oryear: " + dt.ToString("yyyy") + ", branchcode: " + branchcode + ", zonecode: " + zonecode + ", series: 1");
                                trans.Commit();
                                int ser = 1;
                                conn.Close();
                                return dt.ToString("yy") + "-" + ser.ToString().PadLeft(6, '0');
                            }
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    trans.Rollback();
                    conn.Close();
                    throw new Exception(ex.ToString());
                }

            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }

    [WebMethod]
    public ChargeResponse getForexRates(String Username, String Password)
    {
        kplog.Info("Username: "+Username+", Password: "+Password);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
        }
        try
        {
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                conn.Open();
                Double forex = 0;
                try
                {
                    using (command = conn.CreateCommand())
                    {
                        String qrygetrate = "Select stan_buy as rate from mlforexrate.globalrateofficial where currname = 'USD' order by datecreated DESC LIMIT 1";
                        command.CommandText = qrygetrate;
                        command.Parameters.Clear();
                        MySqlDataReader rdrgetrate = command.ExecuteReader();
                        if (rdrgetrate.Read())
                        {
                            forex = Convert.ToDouble(rdrgetrate["rate"].ToString());
                        }
                        else
                        {
                            rdrgetrate.Close();
                            conn.Close();
                            kplog.Error("FAILED:: message: No Forex Rate Found!");
                            return new ChargeResponse { respcode = 0, message = "No Forex Rate Found" };
                        }

                        rdrgetrate.Close();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", forexrate: " + forex);
                        return new ChargeResponse { respcode = 1, message = getRespMessage(1), forexrate = forex };
                    }
                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    conn.Close();
                    throw new Exception(ex.ToString());
                }

            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    //[WebMethod]
    //public ChargeResponse getForexRates(String Username, String Password)
    //{
    //    if (!authenticate(Username, Password))
    //    {
    //        return new ChargeResponse { respcode = 7, message = getRespMessage(7) };
    //    }
    //    try
    //    {
    //        using (MySqlConnection conn = dbconGlobal.getConnection())
    //        {
    //            conn.Open();
    //            Double forex = 0;
    //            try
    //            {
    //                using (command = conn.CreateCommand())
    //                {
    //                    String qrygetrate = "Select rate_USD from currency.currates";
    //                    command.CommandText = qrygetrate;
    //                    command.Parameters.Clear();
    //                    MySqlDataReader rdrgetrate = command.ExecuteReader();
    //                    if (rdrgetrate.Read())
    //                    {
    //                        forex = Convert.ToDouble(rdrgetrate["rate_USD"].ToString());
    //                    }
    //                    else
    //                    {
    //                        rdrgetrate.Close();
    //                        conn.Close();
    //                        return new ChargeResponse { respcode = 0, message = "No Forex Rate Found" };
    //                    }

    //                    rdrgetrate.Close();
    //                    conn.Close();
    //                    return new ChargeResponse { respcode = 0, message = getRespMessage(1), forexrate = forex };
    //                }
    //            }
    //            catch (MySqlException ex)
    //            {
    //                kplog.Fatal(ex.ToString());
    //                conn.Close();
    //                throw new Exception(ex.ToString());
    //            }

    //        }
    //    }
    //    catch (Exception ex)
    //    {
    //        kplog.Fatal(ex.ToString());
    //        throw new Exception(ex.ToString());
    //    }
    //}

    [WebMethod]
    public serviceRates.getRateSettingResponse getRateSettings(String Username, String Password, String branchid, String currency)
    {
        kplog.Info("Username: "+Username+"Password: "+Password+", branchid: "+branchid+", currency: "+currency);
        serviceRates.getRateSetting settings = new serviceRates.getRateSetting();
        serviceRates.getRateSettingResponse response = new serviceRates.getRateSettingResponse();
        serviceRates.ForexWSRemoteClient client = new serviceRates.ForexWSRemoteClient();

        settings.branchid = branchid;
        settings.currency = currency;

        response = client.getRateSetting(settings);

        return response;

    }


    [WebMethod]
    public String generateORNODomestic(string branchcode, string zonecode, Double version, String stationcode)
    {
        kplog.Info("branchcode: "+branchcode+", zonecode: "+zonecode+", version: "+version+", stationcode: "+stationcode);

        try
        {
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    throw new Exception("Version does not match!");
            //}
            using (MySqlConnection conn = dbconDomestic.getConnection())
            {
                conn.Open();
                trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                try
                {
                    using (command = conn.CreateCommand())
                    {
                        command.Transaction = trans;
                        dt = getServerDateGlobal(true);
                        string query = "select oryear,branchcode,zonecode,series from kpforms.resibo where branchcode = @bcode1 and zonecode = @zcode1 FOR UPDATE";
                        command.CommandText = query;
                        command.Parameters.AddWithValue("bcode1", branchcode);
                        command.Parameters.AddWithValue("zcode1", zonecode);
                        //trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                        using (MySqlDataReader dataReader = command.ExecuteReader())
                        {
                            if (dataReader.HasRows)
                            {

                                dataReader.Read();
                                Int32 series = Convert.ToInt32(dataReader["series"]) + 1;
                                String oryear = dataReader["oryear"].ToString().Substring(2);
                                dataReader.Close();
                                if (isSameYear2(dt))
                                {
                                    command.Parameters.Clear();
                                    command.CommandText = "update kpforms.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                                    command.Parameters.AddWithValue("bcode2", branchcode);
                                    command.Parameters.AddWithValue("zcode2", zonecode);
                                    command.Parameters.AddWithValue("series", series);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpforms.resibo : series: " + series + ", branchcode: " + branchcode + ", zoncode: " + zonecode);
                                    trans.Commit();
                                    conn.Close();
                                    return oryear + "-" + series.ToString().PadLeft(6, '0');
                                }
                                else
                                {
                                    kplog.Info("SUCCESS:: message: SERIES RESETTED");
                                    command.Parameters.Clear();
                                    command.CommandText = "update kpforms.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                                    command.Parameters.AddWithValue("bcode2", branchcode);
                                    command.Parameters.AddWithValue("zcode2", zonecode);
                                    command.Parameters.AddWithValue("series", 1);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpforms.resibo : series: 1" + ", branchcode: " + branchcode + ", zonecode: " + zonecode);
                                    trans.Commit();
                                    conn.Close();
                                    return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');
                                }

                            }
                            else
                            {
                                dataReader.Close();
                                //String oryear = dataReader["oryear"].ToString().Substring(2);
                                //dataReader.Close();
                                command.Parameters.Clear();
                                command.CommandText = "update kpforms.resibo set `lock` = 1 where branchcode = @bcode2 and zonecode = @zcode2";
                                command.Parameters.AddWithValue("bcode2", branchcode);
                                command.Parameters.AddWithValue("zcode2", zonecode);
                                command.ExecuteNonQuery();
                                kplog.Info("SUCCESS:: message: update kpforms.resibo : lock: 1,  branchcode: " + branchcode + ", zonecode: " + zonecode);

                                command.Parameters.Clear();
                                command.CommandText = "insert into kpforms.resibo (oryear, branchcode, zonecode, series) values (@year, @bcode2, @zcode2, @ser)";
                                command.Parameters.AddWithValue("year", dt.ToString("yyyy"));
                                command.Parameters.AddWithValue("bcode2", branchcode);
                                command.Parameters.AddWithValue("zcode2", zonecode);
                                command.Parameters.AddWithValue("ser", 1);
                                command.ExecuteNonQuery();
                                kplog.Info("SUCCESS:: message: update kpforms.resibo : series: 1" + ", branchcode: " + branchcode + ", zonecode: " + zonecode + ", oryear: " + dt.ToString("yyyy"));
                                trans.Commit();
                                int ser = 1;
                                conn.Close();
                                return dt.ToString("yy") + "-" + ser.ToString().PadLeft(6, '0');
                            }
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    trans.Rollback();
                    conn.Close();
                    throw new Exception(ex.ToString());
                }

            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }

    //HELPERS

    private String generateResiboGlobal(string branchcode, Int32 zonecode, MySqlCommand command)
    {
        try
        {

            dt = getServerDateGlobal(true);
            //string query = "select oryear,branchcode,zonecode,series from kpformsglobal.resibo where branchcode = @bcode1 and zonecode = @zcode1 and oryear = @oryear FOR UPDATE";
            string query = "select oryear,branchcode,zonecode,series from kpformsglobal.resibo where branchcode = @bcode1 and zonecode = @zcode1 FOR UPDATE";
            command.CommandText = query;
            command.Parameters.AddWithValue("bcode1", branchcode);
            command.Parameters.AddWithValue("zcode1", zonecode);
            //command.Parameters.AddWithValue("oryear", dt.ToString("yyyy"));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                if (dataReader.HasRows)
                {
                    dataReader.Read();
                    Int32 series = Convert.ToInt32(dataReader["series"]) + 1;
                    String oryear = dataReader["oryear"].ToString().Substring(2);
                    dataReader.Close();

                    //if (isSameYear2(dt))
                    //{
                    //    //command.Parameters.Clear();
                    //    //command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                    //    //command.Parameters.AddWithValue("bcode2", branchcode);
                    //    //command.Parameters.AddWithValue("zcode2", zonecode);
                    //    //command.Parameters.AddWithValue("series", series);
                    //    //command.ExecuteNonQuery();
                    //    //command.Parameters.Clear();

                    //    return oryear + "-" + series.ToString().PadLeft(6, '0');
                    //}
                    //else
                    //{
                    //    kplog.Info("SERIES RESETTED");
                    //    //command.Parameters.Clear();
                    //    //command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                    //    //command.Parameters.AddWithValue("bcode2", branchcode);
                    //    //command.Parameters.AddWithValue("zcode2", zonecode);
                    //    //command.Parameters.AddWithValue("series", 1);
                    //    //command.ExecuteNonQuery();
                    //    //command.Parameters.Clear();

                    //    return oryear + "-" + series.ToString().PadLeft(6, '0');
                    //}

                    return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');

                }
                else
                {
                    dataReader.Close();
                    //String oryear = dataReader["oryear"].ToString().Substring(2);
                    //dataReader.Close();
                    command.Parameters.Clear();
                    command.CommandText = "update kpformsglobal.resibo set `lock` = 1 where branchcode = @bcode2 and zonecode = @zcode2";
                    command.Parameters.AddWithValue("bcode2", branchcode);
                    command.Parameters.AddWithValue("zcode2", zonecode);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message: update kpforms.resibo : lock: 1,  branchcode: " + branchcode + ", zonecode: " + zonecode);

                    command.Parameters.Clear();
                    command.CommandText = "insert into kpformsglobal.resibo (oryear, branchcode, zonecode, series) values (@year, @bcode2, @zcode2, @ser)";
                    command.Parameters.AddWithValue("year", dt.ToString("yyyy"));
                    command.Parameters.AddWithValue("bcode2", branchcode);
                    command.Parameters.AddWithValue("zcode2", zonecode);
                    command.Parameters.AddWithValue("ser", 1);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message: update kpforms.resibo : series: 1,  branchcode: " + branchcode + ", zonecode: " + zonecode + ", oryear: " + dt.ToString("yyyy"));
                    int ser = 1;

                    return dt.ToString("yy") + "-" + ser.ToString().PadLeft(6, '0');
                }
            }

        }
        catch (MySqlException myx)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
            throw new Exception(myx.ToString());
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }

    //private String generateResiboGlobal(string branchcode, Int32 zonecode, MySqlCommand command)
    //{
    //    try
    //    {

    //        dt = getServerDateGlobal(true);
    //        string query = "select oryear,branchcode,zonecode,series from kpformsglobal.resibo where branchcode = @bcode1 and zonecode = @zcode1 FOR UPDATE";
    //        command.CommandText = query;
    //        command.Parameters.AddWithValue("bcode1", branchcode);
    //        command.Parameters.AddWithValue("zcode1", zonecode);

    //        using (MySqlDataReader dataReader = command.ExecuteReader())
    //        {
    //            if (dataReader.HasRows)
    //            {
    //                dataReader.Read();
    //                Int32 series = Convert.ToInt32(dataReader["series"]) + 1;
    //                String oryear = dataReader["oryear"].ToString().Substring(2);
    //                dataReader.Close();
    //                if (isSameYear2(dt))
    //                {
    //                    //command.Parameters.Clear();
    //                    //command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
    //                    //command.Parameters.AddWithValue("bcode2", branchcode);
    //                    //command.Parameters.AddWithValue("zcode2", zonecode);
    //                    //command.Parameters.AddWithValue("series", series);
    //                    //command.ExecuteNonQuery();
    //                    //command.Parameters.Clear();
    //                    return oryear + "-" + series.ToString().PadLeft(6, '0');
    //                }
    //                else
    //                {
    //                    kplog.Info("SERIES RESETTED");
    //                    //command.Parameters.Clear();
    //                    //command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
    //                    //command.Parameters.AddWithValue("bcode2", branchcode);
    //                    //command.Parameters.AddWithValue("zcode2", zonecode);
    //                    //command.Parameters.AddWithValue("series", 1);
    //                    //command.ExecuteNonQuery();
    //                    //command.Parameters.Clear();
    //                    return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');

    //                }

    //            }
    //            else
    //            {
    //                dataReader.Close();
    //                //String oryear = dataReader["oryear"].ToString().Substring(2);
    //                //dataReader.Close();
    //                command.Parameters.Clear();
    //                command.CommandText = "update kpformsglobal.resibo set `lock` = 1 where branchcode = @bcode2 and zonecode = @zcode2";
    //                command.Parameters.AddWithValue("bcode2", branchcode);
    //                command.Parameters.AddWithValue("zcode2", zonecode);
    //                command.ExecuteNonQuery();

    //                command.Parameters.Clear();
    //                command.CommandText = "insert into kpformsglobal.resibo (oryear, branchcode, zonecode, series) values (@year, @bcode2, @zcode2, @ser)";
    //                command.Parameters.AddWithValue("year", dt.ToString("yyyy"));
    //                command.Parameters.AddWithValue("bcode2", branchcode);
    //                command.Parameters.AddWithValue("zcode2", zonecode);
    //                command.Parameters.AddWithValue("ser", 1);
    //                command.ExecuteNonQuery();
    //                int ser = 1;

    //                return dt.ToString("yy") + "-" + ser.ToString().PadLeft(6, '0');
    //            }
    //        }

    //    }
    //    catch (MySqlException myx)
    //    {
    //        kplog.Fatal(myx.ToString());
    //        throw new Exception(myx.ToString());
    //    }
    //    catch (Exception ex)
    //    {
    //        kplog.Fatal(ex.ToString());
    //        throw new Exception(ex.ToString());
    //    }

    //}

    private String generateResiboDomestic(string branchcode, Int32 zonecode, MySqlCommand command)
    {
        try
        {

            dt = getServerDateDomestic(true);
            string query = "select oryear,branchcode,zonecode,series from kpforms.resibo where branchcode = @bcode1 and zonecode = @zcode1 FOR UPDATE";
            command.CommandText = query;
            command.Parameters.AddWithValue("bcode1", branchcode);
            command.Parameters.AddWithValue("zcode1", zonecode);

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                if (dataReader.HasRows)
                {
                    dataReader.Read();
                    Int32 series = Convert.ToInt32(dataReader["series"]) + 1;
                    String oryear = dataReader["oryear"].ToString().Substring(2);
                    dataReader.Close();
                    if (isSameYear2(dt))
                    {
                        //command.Parameters.Clear();
                        //command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                        //command.Parameters.AddWithValue("bcode2", branchcode);
                        //command.Parameters.AddWithValue("zcode2", zonecode);
                        //command.Parameters.AddWithValue("series", series);
                        //command.ExecuteNonQuery();
                        //command.Parameters.Clear();
                        return oryear + "-" + series.ToString().PadLeft(6, '0');
                    }
                    else
                    {
                        kplog.Info("SUCCESS:: message: SERIES RESETTED");
                        //command.Parameters.Clear();
                        //command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                        //command.Parameters.AddWithValue("bcode2", branchcode);
                        //command.Parameters.AddWithValue("zcode2", zonecode);
                        //command.Parameters.AddWithValue("series", 1);
                        //command.ExecuteNonQuery();
                        //command.Parameters.Clear();
                        return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');

                    }

                }
                else
                {
                    dataReader.Close();
                    //String oryear = dataReader["oryear"].ToString().Substring(2);
                    //dataReader.Close();
                    command.Parameters.Clear();
                    command.CommandText = "update kpforms.resibo set `lock` = 1 where branchcode = @bcode2 and zonecode = @zcode2";
                    command.Parameters.AddWithValue("bcode2", branchcode);
                    command.Parameters.AddWithValue("zcode2", zonecode);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message: update kpforms.resibo : lock: 1,  branchcode: " + branchcode + ", zonecode: " + zonecode);

                    command.Parameters.Clear();
                    command.CommandText = "insert into kpforms.resibo (oryear, branchcode, zonecode, series) values (@year, @bcode2, @zcode2, @ser)";
                    command.Parameters.AddWithValue("year", dt.ToString("yyyy"));
                    command.Parameters.AddWithValue("bcode2", branchcode);
                    command.Parameters.AddWithValue("zcode2", zonecode);
                    command.Parameters.AddWithValue("ser", 1);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message: update kpforms.resibo : oryear: " + dt.ToString("yyyy") + ",  branchcode: " + branchcode + ", zonecode: " + zonecode);
                    int ser = 1;

                    return dt.ToString("yy") + "-" + ser.ToString().PadLeft(6, '0');
                }
            }

        }
        catch (MySqlException myx)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
            throw new Exception(myx.ToString());
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }

    private Boolean updateResiboGlobal(string branchcode, Int32 zonecode, String resibo, ref MySqlCommand command)
    {
        try
        {
            MySqlCommand cmdReader;
            using (cmdReader = dbconGlobal.getConnection().CreateCommand())
            {

                dt = getServerDateGlobal(true);

                Int32 series = Convert.ToInt32(resibo.Substring(3, resibo.Length - 3));

                //if (isSameYear2(dt))
                //{
                //    command.Parameters.Clear();
                //    //sendout_update_resibo proc(IN branchcode VARCHAR(3), IN zonecode INT(3), IN resibo INT(6),IN issameyr TINYINT(1))
                //    //issameyr=1;
                //    command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2 and oryear = @oryear";
                //    command.Parameters.AddWithValue("bcode2", branchcode);
                //    command.Parameters.AddWithValue("zcode2", zonecode);
                //    command.Parameters.AddWithValue("series", series);
                //    command.Parameters.AddWithValue("oryear", dt.ToString("yyyy"));
                //    command.ExecuteNonQuery();
                //    command.Parameters.Clear();
                //    return true;
                //}
                //else
                //{
                //    command.Parameters.Clear();
                //    //sendout_update_resibo proc(IN branchcode VARCHAR(3), IN zonecode INT(3), IN resibo INT(6),IN issameyr TINYINT(1))
                //    //issameyr=0;
                //    command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2 and oryear = @oryear";
                //    command.Parameters.AddWithValue("bcode2", branchcode);
                //    command.Parameters.AddWithValue("zcode2", zonecode);
                //    command.Parameters.AddWithValue("series", series);
                //    command.Parameters.AddWithValue("oryear", dt.ToString("yyyy"));
                //    command.ExecuteNonQuery();
                //    command.Parameters.Clear();
                //    //return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');
                //    return true;
                //}

                command.Parameters.Clear();
                command.CommandText = "update kpformsglobal.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                command.Parameters.AddWithValue("bcode2", branchcode);
                command.Parameters.AddWithValue("zcode2", zonecode);
                command.Parameters.AddWithValue("series", series);
                command.ExecuteNonQuery();
                command.Parameters.Clear();

                kplog.Info("SUCCESS:: message: UPDATE kpformsglobal.RESIBO:: branchcode: " + branchcode + " zonecode: " + zonecode + " series: " + series);
                return true;


            }
        }
        catch (MySqlException myx)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
            throw new Exception(myx.ToString());
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }

    private Boolean updateResiboDomestic(string branchcode, Int32 zonecode, String resibo, ref MySqlCommand command)
    {
        try
        {
            MySqlCommand cmdReader;
            using (cmdReader = dbconDomestic.getConnection().CreateCommand())
            {

                dt = getServerDateDomestic(true);

                Int32 series = Convert.ToInt32(resibo.Substring(3, resibo.Length - 3));

                if (isSameYear2(dt))
                {
                    command.Parameters.Clear();
                    command.CommandText = "update kpforms.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                    command.Parameters.AddWithValue("bcode2", branchcode);
                    command.Parameters.AddWithValue("zcode2", zonecode);
                    command.Parameters.AddWithValue("series", series);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message: UPDATE kpformsglobal.RESIBO:: branchcode: " + branchcode + " zonecode: " + zonecode + " series: " + series);
                    command.Parameters.Clear();
                    return true;
                }
                else
                {
                    command.Parameters.Clear();
                    command.CommandText = "update kpforms.resibo set series = @series where branchcode = @bcode2 and zonecode = @zcode2";
                    command.Parameters.AddWithValue("bcode2", branchcode);
                    command.Parameters.AddWithValue("zcode2", zonecode);
                    command.Parameters.AddWithValue("series", 1);
                    command.ExecuteNonQuery();
                    kplog.Info("SUCCESS:: message: UPDATE kpformsglobal.RESIBO:: branchcode: " + branchcode + " zonecode: " + zonecode + " series: 1");
                    command.Parameters.Clear();
                    //return dt.ToString("yy") + "-" + series.ToString().PadLeft(6, '0');
                    return true;
                }


            }
        }
        catch (MySqlException myx)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
            throw new Exception(myx.ToString());
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }

    private String generateCustIDGlobal(MySqlCommand command)
    {
        //DateTime dt = DateTime.Now;

        try
        {
            //using (command = conn.CreateCommand())
            //{
            //conn.Open();

            dt = getServerDateGlobal(true, command);

            String query = "select series from kpformsglobal.customerseries FOR UPDATE";
            command.CommandText = query;
            MySqlDataReader Reader = command.ExecuteReader();

            Reader.Read();
            String series = Reader["series"].ToString();
            Reader.Close();

            return dt.ToString("yy") + dt.ToString("MM") + series.PadLeft(10, '0');


            //}
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
            //return null;
            //conn.Close();
            //return new ChargeResponse(0, ex.ToString(), 0);
        }

    }

    private String generateCustID(MySqlCommand command)
    {
        //DateTime dt = DateTime.Now;

        try
        {
            //using (command = conn.CreateCommand())
            //{
            //conn.Open();

            dt = getServerDateDomestic(true, command);

            String query = "select series from kpforms.customerseries FOR UPDATE";
            command.CommandText = query;
            MySqlDataReader Reader = command.ExecuteReader();

            Reader.Read();
            String series = Reader["series"].ToString();
            Reader.Close();

            return dt.ToString("yy") + dt.ToString("MM") + series.PadLeft(10, '0');


            //}
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
            //return null;
            //conn.Close();
            //return new ChargeResponse(0, ex.ToString(), 0);
        }

    }

    private DateTime getServerDateDomestic(bool p, MySqlCommand command)
    {
        throw new NotImplementedException();
    }

    private String generateKPTNGlobalMobile(String branchcode, Int32 zonecode)
    {
        try
        {
            String guid = Guid.NewGuid().GetHashCode().ToString();
            Random rand = new Random();
            dt = getServerDateGlobal(false);
            jp.takel.PseudoRandom.MersenneTwister randGen = new jp.takel.PseudoRandom.MersenneTwister((uint)HiResDateTime.UtcNow.Ticks);
            return "MLG" + branchcode + dt.ToString("dd") + zonecode.ToString() + randGen.Next(1, int.MaxValue).ToString().PadLeft(10, '0') + dt.ToString("MM"); ;
            kplog.Info("SUCCESS:: message: MLG" + branchcode + dt.ToString("dd") + zonecode.ToString() + randGen.Next(1, int.MaxValue).ToString().PadLeft(10, '0') + dt.ToString("MM"));
            //return "MLG" + branchcode + dt.ToString("dd") + zonecode.ToString() + "M" + randGen.Next(1, int.MaxValue).ToString().PadLeft(8, '0') + dt.ToString("MM"); ;
        }
        catch (Exception a)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + a.ToString());
            throw new Exception(a.ToString());
        }
    }

    private String generateKPTNGlobal(String branchcode, Int32 zonecode)
    {
        try
        {
            String guid = Guid.NewGuid().GetHashCode().ToString();
            Random rand = new Random();
            dt = getServerDateGlobal(false);
            jp.takel.PseudoRandom.MersenneTwister randGen = new jp.takel.PseudoRandom.MersenneTwister((uint)HiResDateTime.UtcNow.Ticks);
            kplog.Info("SUCCESS: message: MLG" + branchcode + dt.ToString("dd") + zonecode.ToString() + randGen.Next(1, int.MaxValue).ToString().PadLeft(10, '0') + dt.ToString("MM")); 
            return "MLG" + branchcode + dt.ToString("dd") + zonecode.ToString() + randGen.Next(1, int.MaxValue).ToString().PadLeft(10, '0') + dt.ToString("MM"); ;
        }
        catch (Exception a)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + a.ToString());
            throw new Exception(a.ToString());
        }
    }

    private String generateKPTNDomestic(String branchcode, Int32 zonecode)
    {
        try
        {
            //String guid = Guid.NewGuid().GetHashCode().ToString();
            //jp.takel.PseudoRandom.MersenneTwister rand = new jp.takel.PseudoRandom.MersenneTwister();


            dt = getServerDateDomestic(false);
            jp.takel.PseudoRandom.MersenneTwister randGen = new jp.takel.PseudoRandom.MersenneTwister((uint)dt.Ticks);
            return branchcode + dt.ToString("dd") + zonecode.ToString() + randGen.Next(1000000000, Int32.MaxValue).ToString() + dt.ToString("MM"); ;
        }
        catch (Exception a)
        {
          
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + a.ToString());
            throw new Exception(a.ToString());
        }
    }


    private String generateKPTN(String branchcode, String zonecode, String initiatior)
    {
        String guid = Guid.NewGuid().GetHashCode().ToString();
        Random rand = new Random();

        if (initiatior == String.Empty)
        {
            dt = getServerDateGlobal(true);
        }
        else
        {
            dt = getServerDateGlobal(false);
        }
        int x = System.Convert.ToInt32(guid);
        if (guid.Length < 10)
        {
            if (guid.StartsWith("-"))
            {
                //throw new Exception("Less 10:" + bid + rand.Next(1, 9).ToString() + "" + (x * -1));
                //if (guid.Substring(1, guid.Length-1).Length < 10)
                //{
                //throw new Exception(x.ToString());
                //}
                //else {
                if (guid.Length == 8)
                {
                    //throw new Exception("Starts width -:" + bid + rand.Next(10, 99).ToString() + "" + x);
                    kplog.Info("SUCCESS: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(100, 999).ToString() + "" + (x * -1) + dt.ToString("MM"));
                    return branchcode + dt.ToString("dd") + zonecode + rand.Next(100, 999).ToString() + "" + (x * -1) + dt.ToString("MM");
                }
                else if (guid.Length == 7)
                {
                    //throw new Exception("Starts width -:" + bid + rand.Next(10, 99).ToString() + "" + x);
                    kplog.Info("SUCCESS:: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(100, 999).ToString() + "" + (x * -1) + dt.ToString("MM"));
                    return branchcode + dt.ToString("dd") + zonecode + rand.Next(100, 999).ToString() + "" + (x * -1) + dt.ToString("MM");
                }
                else
                {
                    //return branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + x + dt.ToString("MM");
                    kplog.Info("SUCCESS:: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(10, 99).ToString() + "" + (x * -1) + dt.ToString("MM"));
                    return branchcode + dt.ToString("dd") + zonecode + rand.Next(10, 99).ToString() + "" + (x * -1) + dt.ToString("MM");
                }


                //}
            }
            else
            {
                if (guid.Length == 8)
                {
                    //throw new Exception("Starts width -:" + bid + rand.Next(10, 99).ToString() + "" + x);
                    kplog.Info("SUCCESS:: message: " + branchcode + " " + dt.ToString("dd") + " " + zonecode + " " + rand.Next(10, 99).ToString() + " " + x + " " + dt.ToString("MM"));
                    return branchcode + dt.ToString("dd") + zonecode + rand.Next(10, 99).ToString() + "" + x + dt.ToString("MM");
                }

                else if (guid.Length == 7)
                {
                    //throw new Exception("Starts width -:" + bid + rand.Next(10, 99).ToString() + "" + x);
                    kplog.Info("SUCCESS:: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(100, 999).ToString() + "" + x + dt.ToString("MM"));
                    return branchcode + dt.ToString("dd") + zonecode + rand.Next(100, 999).ToString() + "" + x + dt.ToString("MM");
                }
                else
                {
                    kplog.Info("SUCCESS:: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + x + dt.ToString("MM"));
                    return branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + x + dt.ToString("MM");
                }
                //else {
                //    throw new Exception("Less 10:" + bid + rand.Next(1, 9).ToString() + "" + x);
                //}
                //throw new Exception("Less 10:" + bid + rand.Next(1, 9).ToString() + "" + x);

            }
            //return bid + rand.Next(1, 9).ToString() + "" + (x * -1);
        }
        else if (guid.Length > 10)
        {
            //throw new Exception("Greater 10:" + bid + (x * -1));
            return branchcode + dt.ToString("dd") + zonecode + (x * -1) + dt.ToString("MM");
        }
        else
        {
            if (guid.StartsWith("-"))
            {
                //throw new Exception("Starts with: " + bid + rand.Next(1, 9).ToString() + "" + (x*-1));
                kplog.Info("SUCCESS:: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + (x * -1) + dt.ToString("MM"));
                return branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + (x * -1) + dt.ToString("MM");
            }
            else if (guid.Length == 9)
            {
                //throw new Exception("Starts width -:" + bid + rand.Next(10, 99).ToString() + "" + x);
                kplog.Info("SUCCESS:: message : " + branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + (x * -1) + dt.ToString("MM"));
                return branchcode + dt.ToString("dd") + zonecode + rand.Next(1, 9).ToString() + "" + (x * -1) + dt.ToString("MM");
            }
            else if (guid.Length == 8)
            {
                //throw new Exception("Starts width -:" + bid + rand.Next(10, 99).ToString() + "" + x);
                kplog.Info("SUCCESS:: message: " + branchcode + dt.ToString("dd") + zonecode + rand.Next(10, 99).ToString() + "" + x + dt.ToString("MM"));
                return branchcode + dt.ToString("dd") + zonecode + rand.Next(10, 99).ToString() + "" + x + dt.ToString("MM");
            }

            else
            {
                //throw new Exception(guid);
                kplog.Info("SUCCESS: message: " + branchcode + dt.ToString("dd") + zonecode + guid + dt.ToString("MM"));
                return branchcode + dt.ToString("dd") + zonecode + guid + dt.ToString("MM");
            }
        }
    }

    [WebMethod]
    public CustomerResultResponse getfaxtelno(String Username, String Password, String branchcode, String zonecode)
    {
        kplog.Info("Username: "+Username+", Password: "+Password+", branchcode: "+branchcode+", zonecode: "+zonecode);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                throw new Exception("Invalid credentials");
            }
            using (MySqlConnection custconn = CMMSConnectGlobal.getConnection())
            {
                try
                {
                    custconn.Open();
                    using (custcommand = custconn.CreateCommand())
                    {
                        //int counter = 0;
                        String query = "SELECT TelNo, FaxNo FROM kpusersglobal.branches WHERE branchcode=@branchcode AND zonecode=@zonecode";
                        custcommand.CommandText = query;
                        custcommand.Parameters.AddWithValue("branchcode", branchcode);
                        custcommand.Parameters.AddWithValue("zonecode", zonecode);
                        using (MySqlDataReader Reader = custcommand.ExecuteReader())
                        {
                            string telno = String.Empty;
                            string faxno = String.Empty;
                            if (Reader.Read())
                            {
                                telno = Reader["TelNo"].ToString();
                                faxno = Reader["FaxNo"].ToString();
                                Reader.Close();
                                custconn.Close();
                                kplog.Info("SUCCESS: message: " + getRespMessage(1) + ", telnum: " + telno + ", faxno: " + faxno);
                                return new CustomerResultResponse { respcode = 1, message = getRespMessage(1), telnum = telno, faxno = faxno };
                            }
                            else
                            {
                                Reader.Close();
                                custconn.Close();
                                kplog.Error("SUCCESS:: message: No Data Found!");
                                return new CustomerResultResponse { respcode = 0, message = "No Data found" };
                            }
                        }
                    }

                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    custconn.Close();
                    throw new Exception(ex.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : No Data Found!" + " , ErrorDetail: " + ex.ToString());
            return new CustomerResultResponse { respcode = 0, message = "No Data found" };
            throw new Exception(ex.ToString());

        }

    }

    [WebMethod]
    public String getCorpname(String Username, String Password, String branchcode, String zonecode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", branchcode: " + branchcode + ", zonecode: " + zonecode);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                throw new Exception("Invalid credentials");
            }
            using (MySqlConnection custconn = CMMSConnectGlobal.getConnection())
            {
                try
                {
                    custconn.Open();
                    using (custcommand = custconn.CreateCommand())
                    {
                        //int counter = 0;
                        String query = "SELECT address FROM kpusersglobal.branches WHERE branchcode=@branchcode AND zonecode=@zonecode";
                        custcommand.CommandText = query;
                        custcommand.Parameters.AddWithValue("branchcode", branchcode);
                        custcommand.Parameters.AddWithValue("zonecode", zonecode);
                        using (MySqlDataReader Reader = custcommand.ExecuteReader())
                        {
                            String address;
                            if (Reader.Read())
                            {
                                address = Reader["address"].ToString();
                                Reader.Close();
                                custconn.Close();
                                return address;
                            }
                            else
                            {
                                Reader.Close();
                                custconn.Close();
                                return String.Empty;
                            }
                        }
                    }

                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    custconn.Close();
                    throw new Exception(ex.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            return String.Empty;
            throw new Exception(ex.ToString());

        }

    }

    public string getChargeObj(string branchid)
    {
        kplog.Info("branchid: "+branchid);

        {
            String query = "SELECT * FROM (	SELECT c.objid, b.objid AS strbranchid, c.strcurrencyid, IFNULL(c.strchargecurrencyid, c.strcurrencyid) AS strchargecurrencyid, c.dteffectivefrom, IFNULL(c.dteffectiveto, NOW()) AS dteffectiveto, NOW() AS dtcurrent FROM mlkp.tblbranch b INNER JOIN mlkp.tblcharge c ON b.strorganizationid=c.strorganizationid WHERE b.objid='" + branchid + "' AND c.strcurrencyid='PHP')c0 WHERE dtcurrent BETWEEN dteffectivefrom AND dteffectiveto ORDER BY dteffectivefrom DESC LIMIT 1";
            String objid;

            //throw new Exception(query);
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, dbconGlobal.getConnection());
                MySqlDataReader dataReader = cmd.ExecuteReader();
                dataReader.Read();
                objid = (String)dataReader["objid"];

                dataReader.Close();
                return objid;
            }
            catch (MySqlException)
            {
                dbconGlobal.CloseConnection();
                //   throw new Exception("getcharge");
                return null;
            }

            //return null;
        }
    }

    //public class Example
    //{
    //    public string Name { get; set; }
    //    public int Value { get; set; }
    //}
    //[WebMethod]
    //public Example[] GetExamples()
    //{
    //    return new Example[]{
    //      new Example { Name = "Test", Value = 7 },
    //      new Example { Name = "Test 2", Value = 500 }
    //  };
    //}

    private Int32 ConvertSeries(String series)
    {
        return Convert.ToInt32(series);
    }

    [WebMethod(BufferResponse = false)]
    public SendoutResponse sendoutGlobalMobile(String Username, String Password, List<object> values, String series, int syscreator, String branchcode, Int32 zonecode, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int IsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, Int32 type, String ExpiryDate, Double version, String stationcode, String KPTN, double vat, Int32 remotezone, String RemoteBranchCode)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", series: " + series + ", syscreator: " + syscreator + ", branchcode: " + branchcode + ", zonecode: " + zonecode + ", SenderMLCardNO: " + SenderMLCardNO + ", SenderFName: " + SenderFName + ", SenderLName: " + SenderLName + ", SenderMName: " + SenderMName + ", SenderStreet: " + SenderStreet + ", SenderProvinceCity: " + SenderProvinceCity + ", SenderCountry: " + SenderCountry + ", SenderGender: " + SenderGender + ", SenderContactNo: " + SenderContactNo + ", IsSMS: " + IsSMS + ", SenderBirthdatE: " + SenderBirthdate + ", SenderBranchID: " + SenderBranchID + ", RecieverMLCardNO: " + ReceiverMLCardNO + ", RecieverFName: " + ReceiverFName + ", RecieverLName: " + ReceiverLName + ", RecieverMName: " + ReceiverMName + ", RecieverStreet: " + ReceiverStreet + ", RecieverProvinceCity: " + ReceiverProvinceCity + ", RecieverCountry: " + ReceiverCountry + ", RecieverGender: " + ReceiverGender + ", RecieverContactNo: " + ReceiverContactNo + ", RecieverBirthdate: " + ReceiverBirthdate + ", type: " + type + ", ExpiryDate: " + ExpiryDate + ", version: " + version + ", stationcode: " + stationcode + ", KPTN: " + KPTN + ", vat: " + vat + ", remotezone: " + remotezone + ", RemoteBranchCode: " + RemoteBranchCode);

        try
        {
            if (values[2].ToString().Equals("0"))
            {
                kplog.Fatal("FAILED:: respcode: 10, message : " + getRespMessage(13));
                return new SendoutResponse { respcode = 10, message = getRespMessage(13) };
            }
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new SendoutResponse { respcode = 7, message = getRespMessage(7) };
            }

            return (SendoutResponse)saveSendoutGlobalMobile(values, series, syscreator, branchcode, zonecode, SenderMLCardNO, SenderFName, SenderLName, SenderMName, SenderStreet, SenderProvinceCity, SenderCountry, SenderGender, SenderContactNo, IsSMS, SenderBirthdate, SenderBranchID, ReceiverMLCardNO, ReceiverFName, ReceiverLName, ReceiverMName, ReceiverStreet, ReceiverProvinceCity, ReceiverCountry, ReceiverGender, ReceiverContactNo, ReceiverBirthdate, type, ExpiryDate, stationcode, KPTN, vat, remotezone, RemoteBranchCode);

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());

            return new SendoutResponse { respcode = 0, message = ex.Message, ErrorDetail = ex.ToString() };
        }
    }

    private object saveSendoutGlobalMobile(List<Object> values, String series, int syscreator, String bcode, Int32 zonecode, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, Int32 type, String ExpiryDate, String stationcode, String kptn, Double vat, Int32 remotezcode, String RemoteBranchCode)
    {

        try
        {
            dt = getServerDateGlobal(false);
            int sr = ConvertSeries(series);
            int xsave = 0;

            String month = dt.ToString("MM");
            String tblorig = "sendout" + dt.ToString("MM") + dt.ToString("dd");

            String controlno = values[0].ToString();
            String OperatorID = values[1].ToString();
            String station = values[2].ToString();
            String IsRemote = values[3].ToString().Trim();
            String RemoteBranch = values[4].ToString();
            String RemoteOperatorID = values[5].ToString();
            String RemoteReason;

            String ispassword = values[7].ToString();
            String transpassword = values[8].ToString();
            String purpose = values[9].ToString();
            String syscreatr = values[10].ToString();
            String source = values[11].ToString();
            String currency = values[12].ToString();
            Double principal = Convert.ToDouble(values[13]);
            Double charge = Convert.ToDouble(values[14]);
            Double othercharge = Convert.ToDouble(values[15]);
            Double redeem = Convert.ToDouble(values[16]);
            Double total = Convert.ToDouble(values[17]);
            String promo = values[18].ToString();
            String relation = values[19].ToString();
            String message = values[20].ToString();
            String idtype = values[21].ToString();
            String idno = values[22].ToString();
            String pocurrency = values[23].ToString();
            String paymenttype = values[24].ToString();
            String bankname = values[25].ToString();
            String cardcheckno = values[26].ToString();
            String cardcheckexpdate = values[27].ToString();
            Double exchangerate = Convert.ToDouble(values[28]);
            Double poamount = Convert.ToDouble(values[29]);
            String trxntype = values[30].ToString();

            using (MySqlConnection checkinglang = dbconGlobal.getConnection())
            {
                checkinglang.Open();
                Int32 maxontrans = 0;
                try
                {
                    using (command = checkinglang.CreateCommand())
                    {

                        string checkifcontrolexist = "select controlno from " + generateTableNameGlobalMobile(0, null) + " where controlno=@controlno";
                        command.CommandTimeout = 0;
                        command.CommandText = checkifcontrolexist;
                        command.Parameters.AddWithValue("controlno", controlno);
                        MySqlDataReader controlexistreader = command.ExecuteReader();
                        if (controlexistreader.HasRows)
                        {
                            controlexistreader.Close();

                            string getcontrolmax = "select max(substring(controlno,length(controlno)-5,length(controlno))) as max from " + generateTableNameGlobalMobile(0, null) + " where if(isremote=1,remotebranch,branchcode) = @branchcode and stationid = @stationid and if(remotezonecode=0 or remotezonecode is null, zonecode,remotezonecode) =@zonecode";
                            command.CommandText = getcontrolmax;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("branchcode", IsRemote == "1" ? RemoteBranchCode : bcode);
                            command.Parameters.AddWithValue("stationid", station);
                            command.Parameters.AddWithValue("zonecode", remotezcode == 0 ? zonecode : remotezcode);

                            MySqlDataReader controlmaxreader = command.ExecuteReader();
                            if (controlmaxreader.Read())
                            {
                                sr = Convert.ToInt32(controlmaxreader["max"].ToString()) + 1;
                            }
                            controlmaxreader.Close();

                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("st", IsRemote == "1" ? "00" : station);
                            command.Parameters.AddWithValue("bcode", IsRemote == "1" ? RemoteBranchCode : bcode);
                            command.Parameters.AddWithValue("series", sr);
                            command.Parameters.AddWithValue("zcode", remotezcode == 0 ? zonecode : remotezcode);
                            command.Parameters.AddWithValue("tp", type);
                            int abc101 = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpformsglobal.control "
                            + " station: " + IsRemote == "1" ? "00" : station
                            + " bcode: " + IsRemote == "1" ? RemoteBranchCode : bcode
                            + " nseries: " + sr
                            + " zcode: " + (remotezcode == 0 ? zonecode : remotezcode)
                            + " type: " + type);

                        }
                    }
                }
                catch (Exception ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 , message: Error in updaing control: " + ex.ToString() + ", max: " + (maxontrans + 1));
                    throw new Exception("Error sa pg-update sa control!: " + ex.ToString() + "max:" + maxontrans + 1);
                }
                checkinglang.Close();
                dbconGlobal.CloseConnection();
            }

            StringBuilder query = new StringBuilder("Insert into " + generateTableNameGlobalMobile(0, null) + "(");
            List<string> li = new List<string>();
            List<string> param = new List<string>();
            String orno;

            //save_sendout proc
            param.Add("ControlNo");
            param.Add("OperatorID");
            param.Add("StationID");
            param.Add("IsRemote");
            param.Add("RemoteBranch");
            param.Add("RemoteOperatorID");
            param.Add("Reason");
            param.Add("IsPassword");
            param.Add("TransPassword");
            param.Add("Purpose");
            param.Add("syscreator");
            param.Add("Source");
            param.Add("Currency");
            param.Add("Principal");
            param.Add("Charge");
            param.Add("OtherCharge");
            param.Add("Redeem");
            param.Add("Total");
            param.Add("Promo");
            param.Add("Relation");
            param.Add("Message");
            param.Add("IDType");
            param.Add("IDNo");
            param.Add("PreferredCurrency");
            param.Add("PaymentType");
            param.Add("BankName");
            param.Add("CardCheckNo");
            param.Add("CardCheckExpDate");
            param.Add("ExchangeRate");
            param.Add("AmountPO");
            param.Add("TransType");

            param.Add("isClaimed");
            param.Add("IsCancelled");
            param.Add("KPTNNo");
            param.Add("ORNo");
            param.Add("syscreated");
            param.Add("BranchCode");
            param.Add("ZoneCode");
            param.Add("TransDate");
            param.Add("ExpiryDate");
            param.Add("SenderMLCardNo");
            param.Add("SenderFName");
            param.Add("SenderLName");
            param.Add("SenderMName");
            param.Add("SenderName");
            param.Add("SenderStreet");
            param.Add("SenderProvinceCity");
            param.Add("SenderCountry");
            param.Add("SenderGender");
            param.Add("SenderContactNo");
            param.Add("SenderBirthDate");
            param.Add("SenderBranchID");
            param.Add("ReceiverFName");
            param.Add("ReceiverLName");
            param.Add("ReceiverMName");
            param.Add("ReceiverName");
            param.Add("ReceiverStreet");
            param.Add("ReceiverProvinceCity");
            param.Add("ReceiverCountry");
            param.Add("ReceiverGender");
            param.Add("ReceiverContactNo");
            param.Add("ReceiverBirthDate");
            param.Add("SenderIsSMS");
            param.Add("RemoteZoneCode");
            param.Add("vat");

            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                conn.Open();

                trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit=0;";
                        command.ExecuteNonQuery();

                        command.Transaction = trans;


                        for (var f = 0; f < param.Count; f++)
                        {
                            query.Append("`").Append(param[f]).Append("`");
                            if ((f + 1) != param.Count)
                            {
                                query.Append(",");
                            }
                            li.Add(param[f]);

                        }
                        query.Append(") values ( ");

                        for (var f = 0; f < param.Count; f++)
                        {
                            query.Append("@").Append(param[f]);
                            if ((f + 1) != param.Count)
                            {
                                query.Append(", ");
                            }
                            li.Add(param[f]);

                        }
                        query.Append(")");

                    }
                    using (command = conn.CreateCommand())
                    {
                        //13 14 17
                        if (trxntype != "Mobile")
                        {
                            if (Convert.ToDouble(values[13]) == 0 || Convert.ToDouble(values[14]) == 0 || Convert.ToDouble(values[17]) == 0)
                            {
                                kplog.Error("FAILED:: respcode: 15, message: " + getRespMessage(15));
                                return new SendoutResponse { respcode = 15, message = getRespMessage(15) };
                            }
                        }

                        try
                        {
                            RemoteReason = values[6].ToString();
                        }
                        catch (Exception ex)
                        {
                            kplog.Fatal("FAILED:: respcode: 0, message : Remote reason set to null, ErrorDetail : " + ex.ToString());
                           
                            RemoteReason = null;
                        }

                        //Remove to support seccom trappings
                        if (IsRemote.Equals("1"))
                        {
                            orno = generateResiboGlobal(RemoteBranch, zonecode, command);
                        }
                        else
                        {
                            orno = generateResiboGlobal(bcode, zonecode, command);
                        }

                        command.CommandText = query.ToString();

                        for (var x = 0; x < values.Count; x++)
                        {


                            //Tinyint
                            if (x == 3 || x == 7)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToInt32(values[x]));
                            }
                            //Double
                            else if (x == 13 || x == 14 || x == 15 || x == 16 || x == 17)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToDecimal(values[x]));
                            }
                            else if (x == 10)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToInt32(values[x]));
                            }
                            else
                            {
                                command.Parameters.AddWithValue(li[x], values[x]);
                            }

                        }
                        command.Parameters.AddWithValue("IsClaimed", 0);
                        command.Parameters.AddWithValue("IsCancelled", 0);
                        command.Parameters.AddWithValue("ORNo", orno);
                        command.Parameters.AddWithValue("KPTNNo", kptn);
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("BranchCode", bcode);
                        command.Parameters.AddWithValue("ZoneCode", zonecode);
                        command.Parameters.AddWithValue("TransDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("ExpiryDate", ExpiryDate);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", SenderBirthdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", ReceiverBirthdate);
                        command.Parameters.AddWithValue("RemoteZoneCode", remotezcode);
                        command.Parameters.AddWithValue("vat", vat);

                        try
                        {
                            xsave = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message : Insert into " + generateTableNameGlobalMobile(0, null) + ": "
                        + " IsClaimed: " + 0
                        + " IsCancelled: " + 0
                        + " ORNo: " + orno
                        + " KPTNNo: " + kptn
                        + " syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + " BranchCode: " + bcode
                        + " ZoneCode: " + zonecode
                        + " TransDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + " ExpiryDate: " + ExpiryDate
                        + " SenderMLCardNO: " + SenderMLCardNO
                        + " SenderFName: " + SenderFName
                        + " SenderLName: " + SenderLName
                        + " SenderMName: " + SenderMName
                        + " SenderName: " + SenderLName + " , " + SenderFName + " " + SenderMName
                        + " SenderStreet: " + SenderStreet
                        + " SenderProvinceCity: " + SenderProvinceCity
                        + " SenderCountry: " + SenderCountry
                        + " SenderGender: " + SenderGender
                        + " SenderContactNo: " + SenderContactNo
                        + " SenderIsSMS: " + SenderIsSMS
                        + " SenderBirthdate: " + SenderBirthdate
                        + " SenderBranchID: " + SenderBranchID
                        + " ReceiverFName: " + ReceiverFName
                        + " ReceiverLName: " + ReceiverLName
                        + " ReceiverMName: " + ReceiverMName
                        + " ReceiverName: " + ReceiverLName + " , " + ReceiverFName + " " + ReceiverMName
                        + " ReceiverStreet: " + ReceiverStreet
                        + " ReceiverProvinceCity: " + ReceiverProvinceCity
                        + " ReceiverCountry: " + ReceiverCountry
                        + " ReceiverGender: " + ReceiverGender
                        + " ReceiverContactNo: " + ReceiverContactNo
                        + " ReceiverBirthdate: " + ReceiverBirthdate
                        + " RemoteZoneCode: " + remotezcode
                        + " vat: " + vat);
                            if (xsave < 1)
                            {
                                trans.Rollback();
                                dbconGlobal.CloseConnection();

                                kplog.Error("FAILED:: respcode: 12, message: " + getRespMessage(12) + ", ErrorDetail: Review Parameters");
                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                            }
                            else
                            {
                                using (command = conn.CreateCommand())
                                {

                                    dt = getServerDateGlobal(true);

                                    String insert = "Insert into Mobextransactionsglobal.sendout" + month + " (controlno,kptnno,orno,operatorid," +
                                    "stationid,isremote,remotebranch,remoteoperatorid,reason,ispassword,transpassword,purpose,isclaimed,iscancelled," +
                                    "syscreated,syscreator,source,currency,principal,charge,othercharge,redeem,total,promo,senderissms,relation,message," +
                                    "idtype,idno,expirydate,branchcode,zonecode,transdate,sendermlcardno,senderfname,senderlname,sendermname,sendername," +
                                    "senderstreet,senderprovincecity,sendercountry,sendergender,sendercontactno,senderbirthdate,senderbranchid," +
                                    "receiverfname,receiverlname,receivermname,receivername,receiverstreet,receiverprovincecity,receivercountry," +
                                    "receivergender,receivercontactno,receiverbirthdate,vat,remotezonecode,tableoriginated,`year`,pocurrency,poamount,ExchangeRate," +
                                    "paymenttype,bankname,cardcheckno,cardcheckexpdate,TransType) values (@controlno,@kptnno,@orno,@operatorid," +
                                    "@stationid,@isremote,@remotebranch,@remoteoperatorid,@reason,@ispassword,@transpassword,@purpose,@isclaimed,@iscancelled," +
                                    "@syscreated,@syscreator,@source,@currency,@principal,@charge,@othercharge,@redeem,@total,@promo,@senderissms,@relation,@message," +
                                    "@idtype,@idno,@expirydate,@branchcode,@zonecode,@transdate,@sendermlcardno,@senderfname,@senderlname,@sendermname,@sendername," +
                                    "@senderstreet,@senderprovincecity,@sendercountry,@sendergender,@sendercontactno,@senderbirthdate,@senderbranchid," +
                                    "@receiverfname,@receiverlname,@receivermname,@receivername,@receiverstreet,@receiverprovincecity,@receivercountry," +
                                    "@receivergender,@receivercontactno,@receiverbirthdate,@vat,@remotezonecode,@tableoriginated,@yr,@pocurrency,@poamount,@exchangerate," +
                                    "@paymenttype,@bankname,@cardcheckno,@cardcheckexpdate,@transtype)";
                                    command.CommandText = insert;

                                    command.Parameters.AddWithValue("controlno", controlno);
                                    command.Parameters.AddWithValue("kptnno", kptn);
                                    command.Parameters.AddWithValue("orno", orno);
                                    command.Parameters.AddWithValue("operatorid", OperatorID);
                                    command.Parameters.AddWithValue("stationid", station);
                                    command.Parameters.AddWithValue("isremote", IsRemote);
                                    command.Parameters.AddWithValue("remotebranch", RemoteBranch);
                                    command.Parameters.AddWithValue("remoteoperatorid", RemoteOperatorID);
                                    command.Parameters.AddWithValue("reason", RemoteReason);
                                    command.Parameters.AddWithValue("ispassword", ispassword);
                                    command.Parameters.AddWithValue("transpassword", transpassword);
                                    command.Parameters.AddWithValue("purpose", purpose);
                                    command.Parameters.AddWithValue("isclaimed", 0);
                                    command.Parameters.AddWithValue("iscancelled", 0);
                                    command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                                    command.Parameters.AddWithValue("syscreator", syscreator);
                                    command.Parameters.AddWithValue("source", source);
                                    command.Parameters.AddWithValue("currency", currency);
                                    command.Parameters.AddWithValue("principal", principal);
                                    command.Parameters.AddWithValue("charge", charge);
                                    command.Parameters.AddWithValue("othercharge", othercharge);
                                    command.Parameters.AddWithValue("redeem", redeem);
                                    command.Parameters.AddWithValue("total", total);
                                    command.Parameters.AddWithValue("promo", promo);
                                    command.Parameters.AddWithValue("senderissms", SenderIsSMS);
                                    command.Parameters.AddWithValue("relation", relation);
                                    command.Parameters.AddWithValue("message", message);
                                    command.Parameters.AddWithValue("idtype", idtype);
                                    command.Parameters.AddWithValue("idno", idno);
                                    command.Parameters.AddWithValue("expirydate", ExpiryDate);
                                    command.Parameters.AddWithValue("branchcode", bcode);
                                    command.Parameters.AddWithValue("zonecode", zonecode);
                                    command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                                    command.Parameters.AddWithValue("sendermlcardno", SenderMLCardNO);
                                    command.Parameters.AddWithValue("senderfname", SenderFName);
                                    command.Parameters.AddWithValue("senderlname", SenderLName);
                                    command.Parameters.AddWithValue("sendermname", SenderMName);
                                    command.Parameters.AddWithValue("sendername", SenderLName + ", " + SenderFName + " " + SenderMName);
                                    command.Parameters.AddWithValue("senderstreet", SenderStreet);
                                    command.Parameters.AddWithValue("senderprovincecity", SenderProvinceCity);
                                    command.Parameters.AddWithValue("sendercountry", SenderCountry);
                                    command.Parameters.AddWithValue("sendergender", SenderGender);
                                    command.Parameters.AddWithValue("sendercontactno", SenderContactNo);
                                    command.Parameters.AddWithValue("senderbirthdate", SenderBirthdate);
                                    command.Parameters.AddWithValue("senderbranchid", SenderBranchID);
                                    command.Parameters.AddWithValue("receiverfname", ReceiverFName);
                                    command.Parameters.AddWithValue("receiverlname", ReceiverLName);
                                    command.Parameters.AddWithValue("receivermname", ReceiverMName);
                                    command.Parameters.AddWithValue("receivername", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                                    command.Parameters.AddWithValue("receiverstreet", ReceiverStreet);
                                    command.Parameters.AddWithValue("receiverprovincecity", ReceiverProvinceCity);
                                    command.Parameters.AddWithValue("receivercountry", ReceiverCountry);
                                    command.Parameters.AddWithValue("receivergender", ReceiverGender);
                                    command.Parameters.AddWithValue("receivercontactno", ReceiverContactNo);
                                    command.Parameters.AddWithValue("receiverbirthdate", ReceiverBirthdate);
                                    command.Parameters.AddWithValue("chargeto", " ");
                                    command.Parameters.AddWithValue("vat", vat);
                                    command.Parameters.AddWithValue("remotezonecode", remotezcode);
                                    command.Parameters.AddWithValue("tableoriginated", tblorig);
                                    command.Parameters.AddWithValue("yr", dt.ToString("yyyy"));
                                    command.Parameters.AddWithValue("pocurrency", pocurrency);
                                    command.Parameters.AddWithValue("poamount", poamount);
                                    command.Parameters.AddWithValue("exchangerate", exchangerate);
                                    command.Parameters.AddWithValue("paymenttype", paymenttype);
                                    command.Parameters.AddWithValue("bankname", bankname);
                                    command.Parameters.AddWithValue("cardcheckno", cardcheckno);
                                    command.Parameters.AddWithValue("cardcheckexpdate", cardcheckexpdate);
                                    command.Parameters.AddWithValue("transtype", trxntype);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCES:: respcode: 1 , message: Insert into Mobextransactionsglobal.sendout: "
                                  + " controlno : " + controlno
                                  + " kptnno : " + kptn
                                  + " orno : " + orno
                                  + " operatorid : " + OperatorID
                                  + " stationid : " + station
                                  + " isremote : " + IsRemote
                                  + " remotebranch : " + RemoteBranch
                                  + " remoteoperatorid : " + RemoteOperatorID
                                  + " reason : " + RemoteReason
                                  + " ispassword : " + ispassword
                                  + " transpassword : " + transpassword
                                  + " purpose : " + purpose
                                  + " isclaimed : " + 0
                                  + " iscancelled : " + 0
                                  + " syscreated : " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                                  + " syscreator : " + syscreator
                                  + " source : " + source
                                  + " currency : " + currency
                                  + " principal : " + principal
                                  + " charge : " + charge
                                  + " othercharge : " + othercharge
                                  + " redeem : " + redeem
                                  + " total : " + total
                                  + " promo : " + promo
                                  + " senderissms : " + SenderIsSMS
                                  + " relation : " + relation
                                  + " message : " + message
                                  + " idtype : " + idtype
                                  + " idno : " + idno
                                  + " expirydate : " + ExpiryDate
                                  + " branchcode : " + bcode
                                  + " zonecode : " + zonecode
                                  + " transdate : " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                                  + " sendermlcardno : " + SenderMLCardNO
                                  + " senderfname : " + SenderFName
                                  + " senderlname : " + SenderLName
                                  + " sendermname : " + SenderMName
                                  + " sendername : " + SenderLName + " , " + SenderFName + " " + SenderMName
                                  + " senderstreet : " + SenderStreet
                                  + " senderprovincecity : " + SenderProvinceCity
                                  + " sendercountry : " + SenderCountry
                                  + " sendergender : " + SenderGender
                                  + " sendercontactno : " + SenderContactNo
                                  + " senderbirthdate : " + SenderBirthdate
                                  + " senderbranchid : " + SenderBranchID
                                  + " receiverfname : " + ReceiverFName
                                  + " receiverlname : " + ReceiverLName
                                  + " receivermname : " + ReceiverMName
                                  + " receivername : " + ReceiverLName + " , " + ReceiverFName + " " + ReceiverMName
                                  + " receiverstreet : " + ReceiverStreet
                                  + " receiverprovincecity : " + ReceiverProvinceCity
                                  + " receivercountry : " + ReceiverCountry
                                  + " receivergender : " + ReceiverGender
                                  + " receivercontactno : " + ReceiverContactNo
                                  + " receiverbirthdate : " + ReceiverBirthdate
                                  + " chargeto : " + " "
                                  + " vat : " + vat
                                  + " remotezonecode : " + remotezcode
                                  + " tableoriginated : " + tblorig
                                  + " yr : " + dt.ToString("yyyy")
                                  + " pocurrency : " + pocurrency
                                  + " poamount : " + poamount
                                  + " exchangerate : " + exchangerate
                                  + " paymenttype : " + paymenttype
                                  + " bankname : " + bankname
                                  + " cardcheckno : " + cardcheckno
                                  + " cardcheckexpdate : " + cardcheckexpdate
                                  + " transtype : " + trxntype);
                                }
                            }
                        }
                        catch (MySqlException myyyx)
                        {
                            kplog.Fatal("FAILED:: respcode: 0 message : MySQL errorcode: 1062" + " , ErrorDetail: " + myyyx.ToString());
                            if (myyyx.Number == 1062)
                            {
                                kplog.Fatal("mysql errcode: 1062", myyyx);
                                command.Parameters.Clear();
                                if (IsRemote.Equals("1"))
                                {
                                    command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                                    command.Parameters.AddWithValue("st", "00");
                                    command.Parameters.AddWithValue("bcode", RemoteBranch);
                                    command.Parameters.AddWithValue("series", sr + 1);
                                    command.Parameters.AddWithValue("zcode", remotezcode);
                                    command.Parameters.AddWithValue("tp", type);
                                    int x = command.ExecuteNonQuery();
                                    kplog.Info("SUCCES:: respcode: 1 , messagE: update kpformsglobal.control : "
                                   + " st : " + "00"
                                   + " bcode : " + RemoteBranch
                                   + " series : " + (sr + 1)
                                   + " zcode : " + remotezcode
                                   + " tp : " + type);

                                    if (x < 1)
                                    {
                                        kplog.Error("FAILED:: respcode: 1 , message: " + getRespMessage(12) + "ErrorDetail : Review Parameters");
                                        trans.Rollback();
                                        dbconGlobal.CloseConnection();

                                        return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                                    }
                                }
                                else
                                {
                                    command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                                    command.Parameters.AddWithValue("st", station);
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("series", sr + 1);
                                    command.Parameters.AddWithValue("zcode", zonecode);
                                    command.Parameters.AddWithValue("tp", type);
                                    command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.control: "
                                     + " station : " + station
                                    + " bcode : " + bcode
                                    + " series : " + (sr + 1)
                                    + " zcode : " + zonecode
                                    + " type : " + type);
                                    int x = command.ExecuteNonQuery();
                                    if (x < 1)
                                    {
                                        kplog.Error("FAILED:: respcode: 12, message: " + getRespMessage(12) + ", ErrorDetail: Review Parameters");
                                        trans.Rollback();
                                        dbconGlobal.CloseConnection();

                                        return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review parameters." };
                                    }
                                }

                                trans.Commit();

                                conn.Close();
                                kplog.Error("FAILED:: Respcode: 13, message: Problem saving transaction. Please close the sendout window and open again. Thank You!, ErrorDetail: Review parameters");
                                return new SendoutResponse { respcode = 13, message = "Problem saving transaction. Please close the sendout window and open again. Thank you.", ErrorDetail = "Review parameters." };
                            }
                            else
                            {

                                if (myyyx.Number == 1213)
                                {
                                    kplog.Fatal("FAILED:: respcode: 11, message: " + getRespMessage(11) + "ErrorDetail: Problem occured during saving. Please resave the transaction!" + " , mysql errcode: 1213");
                                    trans.Rollback();
                                    dbconGlobal.CloseConnection();

                                    return new SendoutResponse { respcode = 11, message = getRespMessage(11), ErrorDetail = "Problem occured during saving. Please resave the transaction." };
                                }
                                else
                                {
                                    trans.Rollback();
                                    dbconGlobal.CloseConnection();
                                    kplog.Error("FAILED:: respcode: 0, message: " + getRespMessage(0) + ", ErrorDetail: " + myyyx.ToString());
                                    return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = myyyx.ToString() };
                                }
                            }
                        }

                        if (IsRemote.Equals("1"))
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "00");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", remotezcode);
                            command.Parameters.AddWithValue("tp", type);
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpformsglobal.control : "
                                  + " station: " + "00"
                                    + " bcode: " + RemoteBranch
                                    + " nseries: " + (sr + 1)
                                    + " zcode: " + remotezcode
                                    + " type: " + type);
                            if (x < 1)
                            {
                                kplog.Error("FAILED:: respcode: 12, message: " + getRespMessage(12) + ", ErrorDetail: Review Parameters.");
                                trans.Rollback();
                                dbconGlobal.CloseConnection();

                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                            }
                        }
                        else
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", station);
                            command.Parameters.AddWithValue("bcode", bcode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", zonecode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: respcode: 1 , message : update kpformsglobal.control: "
                          + " station: " + station
                          + " bcode: " + bcode
                          + " series: " + sr + 1
                          + " zcode: " + zonecode
                          + " type: " + type);
                            if (x < 1)
                            {
                                kplog.Error("FAILED:: respcode: 12, message: " + getRespMessage(12) + " , ErrorDetail: Review Parameters!~");
                                trans.Rollback();
                                dbconGlobal.CloseConnection();

                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review parameters." };
                            }
                        }

                        //sendout_update_resibo proc
                        if (IsRemote.Equals("1"))
                        {
                            updateResiboGlobal(RemoteBranch, remotezcode, orno, ref command);
                        }
                        else
                        {
                            updateResiboGlobal(bcode, zonecode, orno, ref command);
                        }

                        if (xsave == 1)
                        {
                            String custS = getcustomertable(SenderLName);
                            command.Parameters.Clear();
                            command.CommandText = "kpadminlogsglobal.save_customers";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("tblcustomer", custS);
                            command.Parameters.AddWithValue("kptnno", kptn);
                            command.Parameters.AddWithValue("controlno", controlno);
                            command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("fname", SenderFName);
                            command.Parameters.AddWithValue("lname", SenderLName);
                            command.Parameters.AddWithValue("mname", SenderMName);
                            command.Parameters.AddWithValue("sobranch", SenderBranchID);
                            command.Parameters.AddWithValue("pobranch", "");
                            command.Parameters.AddWithValue("isremote", IsRemote);
                            command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch));
                            command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                            command.Parameters.AddWithValue("status", 0);
                            command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("syscreator", syscreatr);
                            command.Parameters.AddWithValue("customertype", "S");
                            command.Parameters.AddWithValue("amount", total);
                            command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: stored proc: kpadminlogsglobal.save_customers: "
                            + " tblcustomer: " + custS
                            + " kptnno: " + kptn
                            + " controlno: " + controlno
                            + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                            + " fname: " + SenderFName
                            + " lname: " + SenderLName
                            + " mname: " + SenderMName
                            + " sobranch: " + SenderBranchID
                            + " pobranch: " + ""
                            + " isremote: " + IsRemote
                            + " remotebranch: " + DBNull.Value
                            + " cancelledbranch: " + String.Empty
                            + " status: " + 0
                            + " syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                            + " syscreator: " + syscreatr
                            + " customertype: " + "S"
                            + " amount: " + total);

                            String custR = getcustomertable(ReceiverLName);
                            command.Parameters.Clear();
                            command.CommandText = "kpadminlogsglobal.save_customers";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("tblcustomer", custR);
                            command.Parameters.AddWithValue("kptnno", kptn);
                            command.Parameters.AddWithValue("controlno", controlno);
                            command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("fname", ReceiverFName);
                            command.Parameters.AddWithValue("lname", ReceiverLName);
                            command.Parameters.AddWithValue("mname", ReceiverMName);
                            command.Parameters.AddWithValue("sobranch", SenderBranchID);
                            command.Parameters.AddWithValue("pobranch", "");
                            command.Parameters.AddWithValue("isremote", IsRemote);
                            command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch));
                            command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                            command.Parameters.AddWithValue("status", 0);
                            command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("syscreator", syscreatr);
                            command.Parameters.AddWithValue("customertype", "R");
                            command.Parameters.AddWithValue("amount", total);
                            command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: stored proc: kpadminlogsglobal.save_customers: "
                             + " tblcustomer: " + custR
                             + " kptnno: " + kptn
                             + " controlno: " + controlno
                             + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                             + " fname: " + ReceiverFName
                             + " lname: " + ReceiverLName
                             + " mname: " + ReceiverMName
                             + " sobranch: " + SenderBranchID
                             + " pobranch: " + ""
                             + " isremote: " + IsRemote
                             + " remotebranch: " + (DBNull.Value)
                             + " cancelledbranch: " + String.Empty
                             + " status: " + 0
                             + " syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                             + " syscreator: " + syscreatr
                             + " customertype: " + "R"
                             + " amount: " + total);


                            command.Parameters.Clear();
                            command.CommandText = "kpadminlogsglobal.savelog53";
                            command.CommandType = CommandType.StoredProcedure;

                            command.Parameters.AddWithValue("kptnno", kptn);
                            command.Parameters.AddWithValue("action", "SENDOUT");
                            command.Parameters.AddWithValue("isremote", IsRemote);
                            command.Parameters.AddWithValue("txndate", dt);
                            command.Parameters.AddWithValue("stationcode", stationcode);
                            command.Parameters.AddWithValue("stationno", station);
                            command.Parameters.AddWithValue("zonecode", zonecode);
                            command.Parameters.AddWithValue("branchcode", bcode);
                            command.Parameters.AddWithValue("operatorid", OperatorID);
                            command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                            command.Parameters.AddWithValue("remotereason", RemoteReason);
                            command.Parameters.AddWithValue("remotebranch", (RemoteBranchCode.Equals(DBNull.Value)) ? null : RemoteBranchCode);
                            command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                            command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                            command.Parameters.AddWithValue("remotezonecode", remotezcode);
                            command.Parameters.AddWithValue("type", "N");
                            command.ExecuteNonQuery();
                            kplog.Info("SUCCESS: message: stored proc: kpadminlogsglobal.savelog53: "
                             + " kptnno: " + kptn
                             + " action: " + "SENDOUT"
                             + " isremote: " + IsRemote
                             + " txndate: " + dt
                             + " stationcode: " + stationcode
                             + " stationno: " + station
                             + " zonecode: " + zonecode
                             + " branchcode: " + bcode
                             + " operatorid: " + OperatorID
                             + " cancelledreason: " + DBNull.Value
                             + " remotereason: " + RemoteReason
                             + " remotebranch: " + DBNull.Value
                             + " remoteoperator: " + DBNull.Value
                             + " oldkptnno: " + DBNull.Value
                             + " remotezonecode: " + remotezcode
                             + " type: " + "N");
                        }


                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", kptn: " + kptn + ", orno: " + orno + ", transdate: " + dt);
                        return new SendoutResponse { respcode = 1, message = getRespMessage(1), kptn = kptn, orno = orno, transdate = dt };
                    }
                }
                catch (MySqlException myx)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
                    if (myx.Number == 1213)
                    {
                        kplog.Fatal("mysql errcode: 1213");
                        trans.Rollback();
                        dbconGlobal.CloseConnection();
                        kplog.Error("FAILED:: respcode: 11, messagE: " + getRespMessage(11) + ", Error Detail: Problem occured during saving. Please resave the transaction.");
                        return new SendoutResponse { respcode = 11, message = getRespMessage(11), ErrorDetail = "Problem occured during saving. Please resave the transaction." };
                    }
                    else
                    {
                        trans.Rollback();
                        dbconGlobal.CloseConnection();
                        kplog.Fatal("FAILED:: respcode: 0 , message: " + getRespMessage(0) + ", ErrorDetail: " + myx.ToString());
                        return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = myx.ToString() };
                    }
                }
                catch (Exception ex)
                {
                    kplog.Fatal("mysql exception catched");
                    trans.Rollback();
                    dbconGlobal.CloseConnection();
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("Outer exception catched.", ex);
            dbconGlobal.CloseConnection();
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }

    }

    private object saveSendoutGlobal(List<Object> values, String series, int syscreator, String bcode, Int32 zonecode, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, Int32 type, String ExpiryDate, String stationcode, String kptn, Double vat, Int32 remotezcode, String RemoteBranchCode)
    {

        try
        {
            dt = getServerDateGlobal(false);
            int sr = ConvertSeries(series);
            int xsave = 0;

            String month = dt.ToString("MM");
            String tblorig = "sendout" + dt.ToString("MM") + dt.ToString("dd");

            String controlno = values[0].ToString();
            String OperatorID = values[1].ToString();
            String station = values[2].ToString();
            String IsRemote = values[3].ToString().Trim();
            String RemoteBranch = values[4].ToString();
            String RemoteOperatorID = values[5].ToString();
            String RemoteReason;
            String ispassword = values[7].ToString();
            String transpassword = values[8].ToString();
            String purpose = values[9].ToString();
            String syscreatr = values[10].ToString();
            String source = values[11].ToString();
            String currency = values[12].ToString();
            Double principal = Convert.ToDouble(values[13]);
            Double charge = Convert.ToDouble(values[14]);
            Double othercharge = Convert.ToDouble(values[15]);
            Double redeem = Convert.ToDouble(values[16]);
            Double total = Convert.ToDouble(values[17]);
            String promo = values[18].ToString();
            String relation = values[19].ToString();
            String message = values[20].ToString();
            String idtype = values[21].ToString();
            String idno = values[22].ToString();
            String pocurrency = values[23].ToString();
            String paymenttype = values[24].ToString();
            String bankname = values[25].ToString();
            String cardcheckno = values[26].ToString();
            String cardcheckexpdate = values[27].ToString();
            Double exchangerate = Convert.ToDouble(values[28]);
            Double poamount = Convert.ToDouble(values[29]);
            String trxntype = values[30].ToString();

            using (MySqlConnection checkinglang = dbconGlobal.getConnection())
            {
                checkinglang.Open();
                Int32 maxontrans = 0;
                try
                {
                    using (command = checkinglang.CreateCommand())
                    {

                        string checkifcontrolexist = "select controlno from " + generateTableNameGlobal(0, null) + " where controlno=@controlno";
                        command.CommandTimeout = 0;
                        command.CommandText = checkifcontrolexist;
                        command.Parameters.AddWithValue("controlno", controlno);
                        MySqlDataReader controlexistreader = command.ExecuteReader();
                        if (controlexistreader.HasRows)
                        {
                            controlexistreader.Close();

                            string getcontrolmax = "select max(substring(controlno,length(controlno)-5,length(controlno))) as max from " + generateTableNameGlobal(0, null) + " where if(isremote=1,remotebranch,branchcode) = @branchcode and stationid = @stationid and if(remotezonecode=0 or remotezonecode is null, zonecode,remotezonecode) =@zonecode";
                            command.CommandText = getcontrolmax;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("branchcode", IsRemote == "1" ? RemoteBranchCode : bcode);
                            command.Parameters.AddWithValue("stationid", station);
                            command.Parameters.AddWithValue("zonecode", remotezcode == 0 ? zonecode : remotezcode);

                            MySqlDataReader controlmaxreader = command.ExecuteReader();
                            if (controlmaxreader.Read())
                            {
                                sr = Convert.ToInt32(controlmaxreader["max"].ToString()) + 1;
                            }
                            controlmaxreader.Close();

                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("st", IsRemote == "1" ? "00" : station);
                            command.Parameters.AddWithValue("bcode", IsRemote == "1" ? RemoteBranchCode : bcode);
                            command.Parameters.AddWithValue("series", sr);
                            command.Parameters.AddWithValue("zcode", remotezcode == 0 ? zonecode : remotezcode);
                            command.Parameters.AddWithValue("tp", type);
                            int abc101 = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: respcode: 1 , message: update kpformsglobal.control: "
                            + " st : " + IsRemote == "1" ? "00" : station
                            + " bcode : " + IsRemote == "1" ? RemoteBranchCode : bcode
                            + " series : " + sr
                            + " zcode : " + (remotezcode == 0 ? zonecode : remotezcode)
                            + " tp : " + type);

                        }
                    }
                }
                catch (Exception ex)
                {
                    kplog.Error("Error in updating control:: ErrorDetail: " + ex.ToString() + " max: " + maxontrans + 1);
                    throw new Exception("Error sa pg-update sa control!: " + ex.ToString() + "max:" + maxontrans + 1);
                }
                checkinglang.Close();
                dbconGlobal.CloseConnection();
            }

            StringBuilder query = new StringBuilder("Insert into " + generateTableNameGlobal(0, null) + "(");
            List<string> li = new List<string>();
            List<string> param = new List<string>();
            String orno;

            param.Add("ControlNo");
            param.Add("OperatorID");
            param.Add("StationID");
            param.Add("IsRemote");
            param.Add("RemoteBranch");
            param.Add("RemoteOperatorID");
            param.Add("Reason");
            param.Add("IsPassword");
            param.Add("TransPassword");
            param.Add("Purpose");
            param.Add("syscreator");
            param.Add("Source");
            param.Add("Currency");
            param.Add("Principal");
            param.Add("Charge");
            param.Add("OtherCharge");
            param.Add("Redeem");
            param.Add("Total");
            param.Add("Promo");
            param.Add("Relation");
            param.Add("Message");
            param.Add("IDType");
            param.Add("IDNo");
            param.Add("PreferredCurrency");
            param.Add("PaymentType");
            param.Add("BankName");
            param.Add("CardCheckNo");
            param.Add("CardCheckExpDate");
            param.Add("ExchangeRate");
            param.Add("AmountPO");
            param.Add("TransType");

            param.Add("isClaimed");
            param.Add("IsCancelled");
            param.Add("KPTNNo");
            param.Add("ORNo");
            param.Add("syscreated");
            param.Add("BranchCode");
            param.Add("ZoneCode");
            param.Add("TransDate");
            param.Add("ExpiryDate");
            param.Add("SenderMLCardNo");
            param.Add("SenderFName");
            param.Add("SenderLName");
            param.Add("SenderMName");
            param.Add("SenderName");
            param.Add("SenderStreet");
            param.Add("SenderProvinceCity");
            param.Add("SenderCountry");
            param.Add("SenderGender");
            param.Add("SenderContactNo");
            param.Add("SenderBirthDate");
            param.Add("SenderBranchID");
            param.Add("ReceiverFName");
            param.Add("ReceiverLName");
            param.Add("ReceiverMName");
            param.Add("ReceiverName");
            param.Add("ReceiverStreet");
            param.Add("ReceiverProvinceCity");
            param.Add("ReceiverCountry");
            param.Add("ReceiverGender");
            param.Add("ReceiverContactNo");
            param.Add("ReceiverBirthDate");
            param.Add("SenderIsSMS");
            param.Add("RemoteZoneCode");
            param.Add("vat");

            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                conn.Open();

                trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit=0;";
                        command.ExecuteNonQuery();

                        command.Transaction = trans;

                        for (var f = 0; f < param.Count; f++)
                        {
                            query.Append("`").Append(param[f]).Append("`");
                            if ((f + 1) != param.Count)
                            {
                                query.Append(",");
                            }
                            li.Add(param[f]);

                        }
                        query.Append(") values ( ");

                        for (var f = 0; f < param.Count; f++)
                        {
                            query.Append("@").Append(param[f]);
                            if ((f + 1) != param.Count)
                            {
                                query.Append(", ");
                            }
                            li.Add(param[f]);

                        }

                        query.Append(")");
                    }

                    using (command = conn.CreateCommand())
                    {
                        //13 14 17
                        if (trxntype != "Mobile")
                        {
                            if (Convert.ToDouble(values[13]) == 0 || Convert.ToDouble(values[14]) == 0 || Convert.ToDouble(values[17]) == 0)
                            {
                                kplog.Error("FAILED:: respcode: 15, message: " + getRespMessage(15));
                                return new SendoutResponse { respcode = 15, message = getRespMessage(15) };
                            }
                        }

                        try
                        {
                            RemoteReason = values[6].ToString();
                        }
                        catch (Exception)
                        {
                            kplog.Error("FAILED:: message: Error in Remote reason set to null");
                            RemoteReason = null;
                        }

                        //Remove to support seccom trappings
                        if (IsRemote.Equals("1"))
                        {
                            //kptn = validateGeneratedKPTN(RemoteBranch, zonecode, String.Empty);
                            orno = generateResiboGlobal(RemoteBranch, zonecode, command);
                        }
                        else
                        {
                            //kptn = validateGeneratedKPTN(bcode, zonecode, String.Empty);
                            orno = generateResiboGlobal(bcode, zonecode, command);
                        }

                        command.CommandText = query.ToString();

                        for (var x = 0; x < values.Count; x++)
                        {


                            //Tinyint
                            if (x == 3 || x == 7)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToInt32(values[x]));
                            }
                            //Double
                            else if (x == 13 || x == 14 || x == 15 || x == 16 || x == 17)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToDecimal(values[x]));
                            }
                            else if (x == 10)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToInt32(values[x]));
                            }
                            else
                            {
                                command.Parameters.AddWithValue(li[x], values[x]);
                            }

                        }
                        command.Parameters.AddWithValue("IsClaimed", 0);
                        command.Parameters.AddWithValue("IsCancelled", 0);
                        command.Parameters.AddWithValue("ORNo", orno);
                        command.Parameters.AddWithValue("KPTNNo", kptn);
                        command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("BranchCode", bcode);
                        command.Parameters.AddWithValue("ZoneCode", zonecode);
                        command.Parameters.AddWithValue("TransDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("ExpiryDate", ExpiryDate);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", SenderBirthdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", ReceiverBirthdate);
                        command.Parameters.AddWithValue("RemoteZoneCode", remotezcode);
                        command.Parameters.AddWithValue("vat", vat);

                        try
                        {
                            xsave = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: Insert into " + generateTableNameGlobal(0, null) + ": "
                        + " IsClaimed : " + 0
                        + " IsCancelled : " + 0
                        + " ORNo : " + orno
                        + " KPTNNo : " + kptn
                        + " syscreated : " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + " BranchCode : " + bcode
                        + " ZoneCode : " + zonecode
                        + " TransDate : " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + " ExpiryDate : " + ExpiryDate
                        + " SenderMLCardNO : " + SenderMLCardNO
                        + " SenderFName : " + SenderFName
                        + " SenderLName : " + SenderLName
                        + " SenderMName : " + SenderMName
                        + " SenderName : " + SenderLName + " , " + SenderFName + " " + SenderMName
                        + " SenderStreet : " + SenderStreet
                        + " SenderProvinceCity : " + SenderProvinceCity
                        + " SenderCountry : " + SenderCountry
                        + " SenderGender : " + SenderGender
                        + " SenderContactNo : " + SenderContactNo
                        + " SenderIsSMS : " + SenderIsSMS
                        + " SenderBirthdate : " + SenderBirthdate
                        + " SenderBranchID : " + SenderBranchID
                        + " ReceiverFName : " + ReceiverFName
                        + " ReceiverLName : " + ReceiverLName
                        + " ReceiverMName : " + ReceiverMName
                        + " ReceiverName : " + ReceiverLName + " , " + ReceiverFName + " " + ReceiverMName
                        + " ReceiverStreet : " + ReceiverStreet
                        + " ReceiverProvinceCity : " + ReceiverProvinceCity
                        + " ReceiverCountry : " + ReceiverCountry
                        + " ReceiverGender : " + ReceiverGender
                        + " ReceiverContactNo : " + ReceiverContactNo
                        + " ReceiverBirthdate : " + ReceiverBirthdate
                        + " RemoteZoneCode : " + remotezcode
                        + " vat : " + vat
                               );
                            if (xsave < 1)
                            {
                                trans.Rollback();
                                dbconGlobal.CloseConnection();
                                kplog.Error("FAILED:: message: Error in saving " + generateTableNameGlobal(0, null) + ":: respcode: 12 message: " + getRespMessage(12) + "ErrorDetail: Review parameters");
                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                            }
                            else
                            {
                                using (command = conn.CreateCommand())
                                {

                                    dt = getServerDateGlobal(true);

                                    String insert = "Insert into kptransactionsglobal.sendout" + month + " (controlno,kptnno,orno,operatorid," +
                                    "stationid,isremote,remotebranch,remoteoperatorid,reason,ispassword,transpassword,purpose,isclaimed,iscancelled," +
                                    "syscreated,syscreator,source,currency,principal,charge,othercharge,redeem,total,promo,senderissms,relation,message," +
                                    "idtype,idno,expirydate,branchcode,zonecode,transdate,sendermlcardno,senderfname,senderlname,sendermname,sendername," +
                                    "senderstreet,senderprovincecity,sendercountry,sendergender,sendercontactno,senderbirthdate,senderbranchid," +
                                    "receiverfname,receiverlname,receivermname,receivername,receiverstreet,receiverprovincecity,receivercountry," +
                                    "receivergender,receivercontactno,receiverbirthdate,vat,remotezonecode,tableoriginated,`year`,pocurrency,poamount,ExchangeRate," +
                                    "paymenttype,bankname,cardcheckno,cardcheckexpdate,TransType) values (@controlno,@kptnno,@orno,@operatorid," +
                                    "@stationid,@isremote,@remotebranch,@remoteoperatorid,@reason,@ispassword,@transpassword,@purpose,@isclaimed,@iscancelled," +
                                    "@syscreated,@syscreator,@source,@currency,@principal,@charge,@othercharge,@redeem,@total,@promo,@senderissms,@relation,@message," +
                                    "@idtype,@idno,@expirydate,@branchcode,@zonecode,@transdate,@sendermlcardno,@senderfname,@senderlname,@sendermname,@sendername," +
                                    "@senderstreet,@senderprovincecity,@sendercountry,@sendergender,@sendercontactno,@senderbirthdate,@senderbranchid," +
                                    "@receiverfname,@receiverlname,@receivermname,@receivername,@receiverstreet,@receiverprovincecity,@receivercountry," +
                                    "@receivergender,@receivercontactno,@receiverbirthdate,@vat,@remotezonecode,@tableoriginated,@yr,@pocurrency,@poamount,@exchangerate," +
                                    "@paymenttype,@bankname,@cardcheckno,@cardcheckexpdate,@transtype)";
                                    command.CommandText = insert;

                                    command.Parameters.AddWithValue("controlno", controlno);
                                    command.Parameters.AddWithValue("kptnno", kptn);
                                    command.Parameters.AddWithValue("orno", orno);
                                    command.Parameters.AddWithValue("operatorid", OperatorID);
                                    command.Parameters.AddWithValue("stationid", station);
                                    command.Parameters.AddWithValue("isremote", IsRemote);
                                    command.Parameters.AddWithValue("remotebranch", RemoteBranch);
                                    command.Parameters.AddWithValue("remoteoperatorid", RemoteOperatorID);
                                    command.Parameters.AddWithValue("reason", RemoteReason);
                                    command.Parameters.AddWithValue("ispassword", ispassword);
                                    command.Parameters.AddWithValue("transpassword", transpassword);
                                    command.Parameters.AddWithValue("purpose", purpose);
                                    command.Parameters.AddWithValue("isclaimed", 0);
                                    command.Parameters.AddWithValue("iscancelled", 0);
                                    command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                                    command.Parameters.AddWithValue("syscreator", syscreator);
                                    command.Parameters.AddWithValue("source", source);
                                    command.Parameters.AddWithValue("currency", currency);
                                    command.Parameters.AddWithValue("principal", principal);
                                    command.Parameters.AddWithValue("charge", charge);
                                    command.Parameters.AddWithValue("othercharge", othercharge);
                                    command.Parameters.AddWithValue("redeem", redeem);
                                    command.Parameters.AddWithValue("total", total);
                                    command.Parameters.AddWithValue("promo", promo);
                                    command.Parameters.AddWithValue("senderissms", SenderIsSMS);
                                    command.Parameters.AddWithValue("relation", relation);
                                    command.Parameters.AddWithValue("message", message);
                                    command.Parameters.AddWithValue("idtype", idtype);
                                    command.Parameters.AddWithValue("idno", idno);
                                    command.Parameters.AddWithValue("expirydate", ExpiryDate);
                                    command.Parameters.AddWithValue("branchcode", bcode);
                                    command.Parameters.AddWithValue("zonecode", zonecode);
                                    command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                                    command.Parameters.AddWithValue("sendermlcardno", SenderMLCardNO);
                                    command.Parameters.AddWithValue("senderfname", SenderFName);
                                    command.Parameters.AddWithValue("senderlname", SenderLName);
                                    command.Parameters.AddWithValue("sendermname", SenderMName);
                                    command.Parameters.AddWithValue("sendername", SenderLName + ", " + SenderFName + " " + SenderMName);
                                    command.Parameters.AddWithValue("senderstreet", SenderStreet);
                                    command.Parameters.AddWithValue("senderprovincecity", SenderProvinceCity);
                                    command.Parameters.AddWithValue("sendercountry", SenderCountry);
                                    command.Parameters.AddWithValue("sendergender", SenderGender);
                                    command.Parameters.AddWithValue("sendercontactno", SenderContactNo);
                                    command.Parameters.AddWithValue("senderbirthdate", SenderBirthdate);
                                    command.Parameters.AddWithValue("senderbranchid", SenderBranchID);
                                    command.Parameters.AddWithValue("receiverfname", ReceiverFName);
                                    command.Parameters.AddWithValue("receiverlname", ReceiverLName);
                                    command.Parameters.AddWithValue("receivermname", ReceiverMName);
                                    command.Parameters.AddWithValue("receivername", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                                    command.Parameters.AddWithValue("receiverstreet", ReceiverStreet);
                                    command.Parameters.AddWithValue("receiverprovincecity", ReceiverProvinceCity);
                                    command.Parameters.AddWithValue("receivercountry", ReceiverCountry);
                                    command.Parameters.AddWithValue("receivergender", ReceiverGender);
                                    command.Parameters.AddWithValue("receivercontactno", ReceiverContactNo);
                                    command.Parameters.AddWithValue("receiverbirthdate", ReceiverBirthdate);
                                    command.Parameters.AddWithValue("chargeto", " ");
                                    command.Parameters.AddWithValue("vat", vat);
                                    command.Parameters.AddWithValue("remotezonecode", remotezcode);
                                    command.Parameters.AddWithValue("tableoriginated", tblorig);
                                    command.Parameters.AddWithValue("yr", dt.ToString("yyyy"));
                                    command.Parameters.AddWithValue("pocurrency", pocurrency);
                                    command.Parameters.AddWithValue("poamount", poamount);
                                    command.Parameters.AddWithValue("exchangerate", exchangerate);
                                    command.Parameters.AddWithValue("paymenttype", paymenttype);
                                    command.Parameters.AddWithValue("bankname", bankname);
                                    command.Parameters.AddWithValue("cardcheckno", cardcheckno);
                                    command.Parameters.AddWithValue("cardcheckexpdate", cardcheckexpdate);
                                    command.Parameters.AddWithValue("transtype", trxntype);
                                    command.ExecuteNonQuery();

                                    command.Parameters.Clear();
                                    command.CommandText = "SELECT CustID FROM kpcustomersglobal.customers WHERE FirstName=@xfname AND LastName=@xlname AND MiddleName=@xmname";
                                    command.Parameters.AddWithValue("xfname", SenderFName);
                                    command.Parameters.AddWithValue("xlname", SenderLName);
                                    command.Parameters.AddWithValue("xmname", SenderMName);
                                    MySqlDataReader rdrcust = command.ExecuteReader();
                                    if (rdrcust.Read())
                                    {
                                        String xxscustid = rdrcust["CustID"].ToString();
                                        rdrcust.Close();

                                        command.Parameters.Clear();
                                        command.CommandText = "UPDATE " + generateTableNameGlobal(0, null) + " SET SenderCustID=@xcustid WHERE KPTNNo=@xkptn";
                                        command.Parameters.AddWithValue("xkptn", kptn);
                                        command.Parameters.AddWithValue("xcustid", xxscustid);
                                        command.ExecuteNonQuery();
                                    }
                                    rdrcust.Close();

                                    kplog.Info("SUCCESS:: message: INSERT INTO kptransactionsglobal.sendout" + month + ":: controlno: " + controlno + " kptnno: " + kptn + " operatorid: " + OperatorID + " stationid: " + station + " isremote: " + IsRemote + " remotebranch: " + RemoteBranch + " remoteoperatorid: " + RemoteOperatorID + " reason: " + RemoteReason + " ispassword: " + ispassword + " transpassword: " + transpassword + " purpose: " + purpose + " isclaimed: 0 iscancelled: 0 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreator + " source: " + source + " currency: " + currency + " principal: " + principal + " charge: " + charge + " othercharge: " + othercharge + " redeem: " + redeem + " total: " + total + " promo: " + promo + " senderissms: " + SenderIsSMS + " relation: " + relation + " message: " + message + " idtype: " + idtype + " idno: " + idno + " expirydate: " + ExpiryDate + " branchcode: " + bcode + " zonecode: " + zonecode + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " sendermlcardno: " + SenderMLCardNO + " senderfname: " + SenderFName + " senderlname: " + SenderLName + " sendermname: " + SenderMName + " sendername: " + SenderLName + ", " + SenderFName + " " + SenderMName + " senderstreet: " + SenderStreet + " senderprovincecity: " + SenderProvinceCity + " sendercountry: " + SenderCountry + " sendergender: " + SenderGender + " sendercontactno: " + SenderContactNo + " senderbirthdate: " + SenderBirthdate + " senderbranchid: " + SenderBranchID + " receiverfname: " + ReceiverFName + " receiverlname: " + ReceiverLName + " receivermname: " + ReceiverMName + " receivername: " + ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName + " receiverstreet: " + ReceiverStreet + " receivercountry: " + ReceiverCountry + " receivergender: " + ReceiverGender + " receivercontactno: " + ReceiverContactNo + " receiverbirthdate: " + ReceiverBirthdate + " chargeto: vat: " + vat + " remotezonecode: " + remotezcode + " tableoriginated: " + tblorig + " yr: " + dt.ToString("yyyy") + " pocurrency: " + pocurrency + " poamount: " + poamount + " exchangerate: " + exchangerate + " paymenttype: " + paymenttype + " bankname: " + bankname + " cardcheckno: " + cardcheckno + " cardcheckexpdate: " + cardcheckexpdate + " transtype: " + trxntype);
                                
                                }
                            }
                        }
                        catch (MySqlException myyyx)
                        {
                            //if (myyyx.Message.Contains("Duplicate"))
                            if (myyyx.Number == 1062)
                            {
                                kplog.Fatal("Error catch:: mysql errcode: 1062 ErrorDetail: " + myyyx.ToString());
                                command.Parameters.Clear();
                                //sendout_update_control proc
                                if (IsRemote.Equals("1"))
                                {
                                    command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                                    command.Parameters.AddWithValue("st", "00");
                                    command.Parameters.AddWithValue("bcode", RemoteBranch);
                                    command.Parameters.AddWithValue("series", sr + 1);
                                    command.Parameters.AddWithValue("zcode", remotezcode);
                                    command.Parameters.AddWithValue("tp", type);
                                    int x = command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.control:  "
                                     + " station: " + "00"
                                     + " bcode: " + RemoteBranch
                                     + " nseries: " + (sr + 1)
                                     + " zcode: " + remotezcode
                                     + " type: " + type);
                                    if (x < 1)
                                    {
                                        kplog.Error("FAILED:: message: Error in updating control:: respcode: 12 message: " + getRespMessage(12) + " ErrorDetail: Review parameters.");
                                        trans.Rollback();
                                        dbconGlobal.CloseConnection();
                                        return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                                    }
                                }
                                else
                                {
                                    command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                                    command.Parameters.AddWithValue("st", station);
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("series", sr + 1);
                                    command.Parameters.AddWithValue("zcode", zonecode);
                                    command.Parameters.AddWithValue("tp", type);
                                    command.ExecuteNonQuery();
                                    int x = command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpformsglobal.control: "
                                    + " station: " + station
                                    + " bcode: " + bcode
                                    + " nseries: " + (sr + 1)
                                    + " zcode: " + zonecode
                                    + " type: " + type);
                                    if (x < 1)
                                    {
                                        kplog.Error("FAILED:: message: Error in updating control:: respcode: 12 message: " + getRespMessage(12) + " ErrorDetail: Review parameters.");
                                        trans.Rollback();
                                        dbconGlobal.CloseConnection();
                                        return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review parameters." };
                                    }
                                }

                                kplog.Error("FAILED:: message: Problem saving transaction:: respcode: 13 ErrorDetail: Review parameters.");
                                trans.Commit();
                                conn.Close();
                                return new SendoutResponse { respcode = 13, message = "Problem saving transaction. Please close the sendout window and open again. Thank you.", ErrorDetail = "Review parameters." };
                            }
                            else
                            {

                                if (myyyx.Number == 1213)
                                {
                                    kplog.Error("FAILED::Error in catch:: mysql errcode: 1213 ErrorDetail: " + myyyx.ToString());
                                    trans.Rollback();
                                    dbconGlobal.CloseConnection();
                                    return new SendoutResponse { respcode = 11, message = getRespMessage(11), ErrorDetail = "Problem occured during saving. Please resave the transaction." };
                                }
                                else
                                {
                                    kplog.Error("FAILED::Error in data:: ErrorDetail: " + myyyx.ToString());
                                    trans.Rollback();
                                    dbconGlobal.CloseConnection();
                                    return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = myyyx.ToString() };
                                }
                            }
                        }

                        if (IsRemote.Equals("1"))
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "00");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", remotezcode);
                            command.Parameters.AddWithValue("tp", type);
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpformsglobal.control: "
                                   + " st: " + "00"
                                   + " bcode: " + RemoteBranch
                                   + " series: " + (sr + 1)
                                   + " zcode: " + remotezcode
                                   + " type: " + type);
                            if (x < 1)
                            {
                                kplog.Error("FAILED:: messagE: Error in updating control:: respcode: 12 message: " + getRespMessage(12) + "ErrorDetail: Review paramerters.");
                                trans.Rollback();
                                dbconGlobal.CloseConnection();
                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };

                            }
                        }
                        else
                        {
                            command.CommandText = "update kpformsglobal.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", station);
                            command.Parameters.AddWithValue("bcode", bcode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", zonecode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpformsglobal.control: "
                                    + " station: " + station
                                    + " bcode: " + bcode
                                    + " series: " + (sr + 1)
                                    + " zcode: " + zonecode
                                    + " type: " + type);
                            if (x < 1)
                            {
                                kplog.Error("FAILED::Error in updating control:: respcode: 12 message: " + getRespMessage(12) + "ErrorDetail: Review paramerters.");
                                trans.Rollback();
                                dbconGlobal.CloseConnection();
                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review parameters." };
                            }
                        }

                        //sendout_update_resibo proc
                        if (IsRemote.Equals("1"))
                        {
                            //kptn = validateGeneratedKPTN(RemoteBranch, zonecode, String.Empty);
                            updateResiboGlobal(RemoteBranch, remotezcode, orno, ref command);
                        }
                        else
                        {
                            //kptn = validateGeneratedKPTN(bcode, zonecode, String.Empty);
                            updateResiboGlobal(bcode, zonecode, orno, ref command);
                        }

                        if (xsave == 1)
                        {
                            //if (trxntype != "Mobile")
                            //{
                            String custS = getcustomertable(SenderLName);
                            command.Parameters.Clear();
                            command.CommandText = "kpadminlogsglobal.save_customers";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("tblcustomer", custS);
                            command.Parameters.AddWithValue("kptnno", kptn);
                            command.Parameters.AddWithValue("controlno", controlno);
                            command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("fname", SenderFName);
                            command.Parameters.AddWithValue("lname", SenderLName);
                            command.Parameters.AddWithValue("mname", SenderMName);
                            command.Parameters.AddWithValue("sobranch", SenderBranchID);
                            command.Parameters.AddWithValue("pobranch", "");
                            command.Parameters.AddWithValue("isremote", IsRemote);
                            command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch));
                            command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                            command.Parameters.AddWithValue("status", 0);
                            command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("syscreator", syscreatr);
                            command.Parameters.AddWithValue("customertype", "S");
                            command.Parameters.AddWithValue("amount", total);
                            command.ExecuteNonQuery();

                            String custR = getcustomertable(ReceiverLName);
                            command.Parameters.Clear();
                            command.CommandText = "kpadminlogsglobal.save_customers";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("tblcustomer", custR);
                            command.Parameters.AddWithValue("kptnno", kptn);
                            command.Parameters.AddWithValue("controlno", controlno);
                            command.Parameters.AddWithValue("transdate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("fname", ReceiverFName);
                            command.Parameters.AddWithValue("lname", ReceiverLName);
                            command.Parameters.AddWithValue("mname", ReceiverMName);
                            command.Parameters.AddWithValue("sobranch", SenderBranchID);
                            command.Parameters.AddWithValue("pobranch", "");
                            command.Parameters.AddWithValue("isremote", IsRemote);
                            command.Parameters.AddWithValue("remotebranch", (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch));
                            command.Parameters.AddWithValue("cancelledbranch", String.Empty);
                            command.Parameters.AddWithValue("status", 0);
                            command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                            command.Parameters.AddWithValue("syscreator", syscreatr);
                            command.Parameters.AddWithValue("customertype", "R");
                            command.Parameters.AddWithValue("amount", total);
                            command.ExecuteNonQuery();
                            //}

                            command.Parameters.Clear();
                            command.CommandText = "kpadminlogsglobal.savelog53";
                            command.CommandType = CommandType.StoredProcedure;

                            command.Parameters.AddWithValue("kptnno", kptn);
                            command.Parameters.AddWithValue("action", "SENDOUT");
                            command.Parameters.AddWithValue("isremote", IsRemote);
                            command.Parameters.AddWithValue("txndate", dt);
                            command.Parameters.AddWithValue("stationcode", stationcode);
                            command.Parameters.AddWithValue("stationno", station);
                            command.Parameters.AddWithValue("zonecode", zonecode);
                            command.Parameters.AddWithValue("branchcode", bcode);
                            command.Parameters.AddWithValue("operatorid", OperatorID);
                            command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                            command.Parameters.AddWithValue("remotereason", RemoteReason);
                            command.Parameters.AddWithValue("remotebranch", (RemoteBranchCode.Equals(DBNull.Value)) ? null : RemoteBranchCode);
                            command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                            command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                            command.Parameters.AddWithValue("remotezonecode", remotezcode);
                            command.Parameters.AddWithValue("type", "N");
                            command.ExecuteNonQuery();

                            kplog.Info("SUCCESS:: INSERT SENDER INFO LOGS:: tblcustomer: " + custS + " kptn: " + kptn + " controlno: " + controlno + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " fname: " + SenderFName + " lname: " + SenderLName + " mname: " + SenderMName + " SObranch: " + SenderBranchID + " pobranch: isremote: " + IsRemote + " remotebranch: " + (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch) + " cancelledbranch: status: 0 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreatr + " customertype: S amount: " + total);
                            kplog.Info("SUCCESS:: INSERT RECEIVER INFO LOGS:: tblcustomer: " + custR + " kptn: " + kptn + " controlno: " + controlno + " transdate: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " fname: " + ReceiverFName + " lname: " + ReceiverLName + " mname: " + ReceiverMName + " SObranch: " + SenderBranchID + " pobranch: isremote: " + IsRemote + " remotebranch: " + (RemoteBranch.Equals(DBNull.Value) ? null : RemoteBranch) + " cancelledbranch: status: 0 syscreated: " + dt.ToString("yyyy-MM-dd HH:mm:ss") + " syscreator: " + syscreatr + " customertype: R amount: " + total);
                            kplog.Info("SUCCESS:: INSERT TRANSACTION LOGS:: kptnno: " + kptn + " action: SENDOUT isremote: " + IsRemote + " txndate: " + dt + " stationcode: " + stationcode + " stationno: " + station + " zonecode: " + zonecode + " branchcode: " + bcode + " operatorid: " + OperatorID + " cancelledreason: " + DBNull.Value + " remotereason: " + RemoteReason + " remotebranch: " + (RemoteBranchCode.Equals(DBNull.Value) ? null : RemoteBranchCode) + " remoteoperator: " + (RemoteOperatorID.Equals(DBNull.Value) ? null : RemoteOperatorID) + " oldkptnno: " + DBNull.Value + " remotezonecode: " + remotezcode + " type: N");
                        }

                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + " , kptn: " + kplog + ", orno: " + orno + ", transdate: " + dt);
                        return new SendoutResponse { respcode = 1, message = getRespMessage(1), kptn = kptn, orno = orno, transdate = dt };
                    }
                }
                catch (MySqlException myx)
                {
                    if (myx.Number == 1213)
                    {
                        kplog.Error("FAILED:: message: Error in catch:: mysql errcode: 1213 ErrorDetail: " + myx.ToString());
                        trans.Rollback();
                        dbconGlobal.CloseConnection();
                        return new SendoutResponse { respcode = 11, message = getRespMessage(11), ErrorDetail = "Problem occured during saving. Please resave the transaction." };
                    }
                    else
                    {
         
                        trans.Rollback();
                        dbconGlobal.CloseConnection();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
                        return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = myx.ToString() };
                    }
                }
                catch (Exception ex)
                {
                     kplog.Error("FAILED:: message: mysql exception catched");
                    trans.Rollback();
                    dbconGlobal.CloseConnection();
                    kplog.Fatal("FAILED:: respcode: 0 message : "+getRespMessage(0)+" , ErrorDetail: "+ex.ToString());
                    return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Error("FAILED:: message: Outer exception catched.", ex);
            dbconGlobal.CloseConnection();
               kplog.Fatal("FAILED:: respcode: 0 message : "+getRespMessage(0)+" , ErrorDetail: "+ex.ToString());
            return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }

    }

    private object saveSendoutDomestic(List<Object> values, String series, int syscreator, String bcode, Int32 zonecode, String SenderMLCardNO, String SenderFName, String SenderLName, String SenderMName, String SenderStreet, String SenderProvinceCity, String SenderCountry, String SenderGender, String SenderContactNo, int SenderIsSMS, String SenderBirthdate, String SenderBranchID, String ReceiverMLCardNO, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverStreet, String ReceiverProvinceCity, String ReceiverCountry, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthdate, Int32 type, String ExpiryDate, String stationcode, String kptn, Int32 remotezcode, String RemoteBranchCode)
    {

        try
        {
            //String senderid = "";
            dt = getServerDateDomestic(false);
            int sr = ConvertSeries(series);

            StringBuilder query = new StringBuilder("Insert into " + generateTableNameDomestic(0, null) + "(");
            List<string> li = new List<string>();
            List<string> param = new List<string>();
            String orno;


            param.Add("ControlNo");
            param.Add("OperatorID");
            param.Add("StationID");
            param.Add("IsRemote");
            param.Add("RemoteBranch");
            param.Add("RemoteOperatorID");
            param.Add("Reason");
            param.Add("IsPassword");
            param.Add("TransPassword");
            param.Add("Purpose");
            param.Add("syscreator");
            param.Add("Source");
            param.Add("Currency");
            param.Add("Principal");
            param.Add("Charge");
            param.Add("OtherCharge");
            param.Add("Redeem");
            param.Add("Total");
            param.Add("Promo");
            param.Add("Relation");
            param.Add("Message");
            param.Add("IDType");
            param.Add("IDNo");
            param.Add("isClaimed");
            param.Add("IsCancelled");
            param.Add("KPTNNo");
            param.Add("ORNo");
            //param.Add("kptn4");
            param.Add("BranchCode");
            param.Add("ZoneCode");
            param.Add("TransDate");
            param.Add("ExpiryDate");
            param.Add("SenderMLCardNo");
            param.Add("SenderFName");
            param.Add("SenderLName");
            param.Add("SenderMName");
            param.Add("SenderName");
            param.Add("SenderStreet");
            param.Add("SenderProvinceCity");
            param.Add("SenderCountry");
            param.Add("SenderGender");
            param.Add("SenderContactNo");
            param.Add("SenderBirthDate");
            param.Add("SenderBranchID");
            param.Add("ReceiverMLCardNo");
            param.Add("ReceiverFName");
            param.Add("ReceiverLName");
            param.Add("ReceiverMName");
            param.Add("ReceiverName");
            param.Add("ReceiverStreet");
            param.Add("ReceiverProvinceCity");
            param.Add("ReceiverCountry");
            param.Add("ReceiverGender");
            param.Add("ReceiverContactNo");
            param.Add("ReceiverBirthDate");
            param.Add("SenderIsSMS");
            param.Add("RemoteZoneCode");
            //param.Add("vat");

            using (MySqlConnection conn = dbconDomestic.getConnection())
            {
                conn.Open();

                trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    //throw new Exception("BOOM");
                    using (command = conn.CreateCommand())
                    {
                        command.CommandText = "SET autocommit=0;";
                        command.ExecuteNonQuery();

                        command.Transaction = trans;


                        for (var f = 0; f < param.Count; f++)
                        {
                            query.Append("`").Append(param[f]).Append("`");
                            if ((f + 1) != param.Count)
                            {
                                query.Append(",");
                            }
                            li.Add(param[f]);

                        }
                        query.Append(") values ( ");

                        for (var f = 0; f < param.Count; f++)
                        {
                            query.Append("@").Append(param[f]);
                            if ((f + 1) != param.Count)
                            {
                                query.Append(", ");
                            }
                            li.Add(param[f]);

                        }
                        query.Append(")");

                        //throw new Exception(query.ToString());
                        //Reader.Close();


                        //return li;
                    }
                    //throw new Exception(query.ToString());
                    using (command = conn.CreateCommand())
                    {
                        //conn.Open();

                        //String kptn;
                        String controlno = values[0].ToString();
                        String OperatorID = values[1].ToString();
                        String station = values[2].ToString();
                        String IsRemote = values[3].ToString().Trim();
                        String RemoteBranch = values[4].ToString();
                        String RemoteOperatorID = values[5].ToString();
                        String RemoteReason;
                        //13 14 17

                        if (Convert.ToDouble(values[13]) == 0 || Convert.ToDouble(values[14]) == 0 || Convert.ToDouble(values[17]) == 0)
                        {
                            kplog.Error("FAILED:: respcode: 15, message: " + getRespMessage(15));
                            return new SendoutResponse { respcode = 15, message = getRespMessage(15) };
                        }

                        try
                        {
                            RemoteReason = values[6].ToString();
                        }
                        catch (Exception)
                        {
                            kplog.Error("FAILED:: message: Remote reason set to null");
                            RemoteReason = null;
                        }

                        //Remove to support seccom trappings
                        if (IsRemote.Equals("1"))
                        {
                            //kptn = validateGeneratedKPTN(RemoteBranch, zonecode, String.Empty);
                            orno = generateResiboDomestic(RemoteBranch, zonecode, command);
                        }
                        else
                        {
                            //kptn = validateGeneratedKPTN(bcode, zonecode, String.Empty);
                            orno = generateResiboDomestic(bcode, zonecode, command);
                        }

                        command.CommandText = query.ToString();

                        for (var x = 0; x < values.Count; x++)
                        {


                            //Tinyint
                            if (x == 3 || x == 7)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToInt32(values[x]));
                            }
                            //Double
                            else if (x == 13 || x == 14 || x == 15 || x == 16 || x == 17)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToDecimal(values[x]));
                            }
                            else if (x == 10)
                            {
                                command.Parameters.AddWithValue(li[x], Convert.ToInt32(values[x]));
                            }
                            else
                            {
                                command.Parameters.AddWithValue(li[x], values[x]);
                            }

                        }
                        //command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("IsClaimed", 0);
                        command.Parameters.AddWithValue("IsCancelled", 0);
                        //command.Parameters.AddWithValue("IsReturned", 0);
                        command.Parameters.AddWithValue("ORNo", orno);
                        //throw new Exception(kptn);
                        command.Parameters.AddWithValue("KPTNNo", kptn);
                        //command.Parameters.AddWithValue("syscreated", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        //command.Parameters.AddWithValue("kptn4", kptn4);
                        command.Parameters.AddWithValue("BranchCode", bcode);
                        command.Parameters.AddWithValue("ZoneCode", zonecode);
                        command.Parameters.AddWithValue("TransDate", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        command.Parameters.AddWithValue("ExpiryDate", ExpiryDate);
                        //command.Parameters.AddWithValue("CustID", senderid);
                        command.Parameters.AddWithValue("SenderMLCardNO", SenderMLCardNO);
                        command.Parameters.AddWithValue("SenderFName", SenderFName);
                        command.Parameters.AddWithValue("SenderLName", SenderLName);
                        command.Parameters.AddWithValue("SenderMName", SenderMName);
                        command.Parameters.AddWithValue("SenderName", SenderLName + ", " + SenderFName + " " + SenderMName);
                        command.Parameters.AddWithValue("SenderStreet", SenderStreet);
                        command.Parameters.AddWithValue("SenderProvinceCity", SenderProvinceCity);
                        command.Parameters.AddWithValue("SenderCountry", SenderCountry);
                        command.Parameters.AddWithValue("SenderGender", SenderGender);
                        command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                        command.Parameters.AddWithValue("SenderIsSMS", SenderIsSMS);
                        command.Parameters.AddWithValue("SenderBirthdate", SenderBirthdate);
                        command.Parameters.AddWithValue("SenderBranchID", SenderBranchID);
                        command.Parameters.AddWithValue("ReceiverMLCardNO", ReceiverMLCardNO);
                        command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                        command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                        command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverName", ReceiverLName + ", " + ReceiverFName + " " + ReceiverMName);
                        command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                        command.Parameters.AddWithValue("ReceiverProvinceCity", ReceiverProvinceCity);
                        command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                        command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                        command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                        command.Parameters.AddWithValue("ReceiverBirthdate", ReceiverBirthdate);
                        command.Parameters.AddWithValue("RemoteZoneCode", remotezcode);
                        //command.Parameters.AddWithValue("vat", vat);
                        try
                        {
                            int xsave = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: Insert into " + generateTableNameDomestic(0, null) + " : "
                        + " IsClaimed: 0"
                        + " IsCancelled: 0"
                        + " ORNo: " + orno
                        + " KPTNNo: " + kptn
                        + " BranchCode: " + bcode
                        + " ZoneCode: " + zonecode
                        + " TransDate: " + dt.ToString("yyyy-MM-dd HH:mm:ss")
                        + " ExpiryDate: " + ExpiryDate
                        + " SenderMLCardNO: " + SenderMLCardNO
                        + " SenderFName: " + SenderFName
                        + " SenderLName: " + SenderLName
                        + " SenderMName: " + SenderMName
                        + " SenderName: " + SenderLName + " , " + SenderFName + " " + SenderMName
                        + " SenderStreet: " + SenderStreet
                        + " SenderProvinceCity: " + SenderProvinceCity
                        + " SenderCountry: " + SenderCountry
                        + " SenderGender: " + SenderGender
                        + " SenderContactNo: " + SenderContactNo
                        + " SenderIsSMS: " + SenderIsSMS
                        + " SenderBirthdate: " + SenderBirthdate
                        + " SenderBranchID: " + SenderBranchID
                        + " ReceiverMLCardNO: " + ReceiverMLCardNO
                        + " ReceiverFName: " + ReceiverFName
                        + " ReceiverLName: " + ReceiverLName
                        + " ReceiverMName: " + ReceiverMName
                        + " ReceiverName: " + ReceiverLName + " , " + ReceiverFName + " " + ReceiverMName
                        + " ReceiverStreet: " + ReceiverStreet
                        + " ReceiverProvinceCity: " + ReceiverProvinceCity
                        + " ReceiverCountry: " + ReceiverCountry
                        + " ReceiverGender: " + ReceiverGender
                        + " ReceiverContactNo: " + ReceiverContactNo
                        + " ReceiverBirthdate: " + ReceiverBirthdate
                        + " RemoteZoneCode: " + remotezcode);
                            if (xsave < 1)
                            {
                                trans.Rollback();
                                dbconDomestic.CloseConnection();
                                kplog.Error("FAILED:: respcode: 12, message: " + getRespMessage(12) + ", ErrorDetail: Review Parameters");
                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                            }

                        }
                        catch (MySqlException myyyx)
                        {
                            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myyyx.ToString());
                            //if (myyyx.Message.Contains("Duplicate"))
                            if (myyyx.Number == 1062)
                            {
                                kplog.Fatal("FAILED:: message: mysql errcode: 1062", myyyx);
                                command.Parameters.Clear();
                                if (IsRemote.Equals("1"))
                                {
                                    //throw new Exception("boom");
                                    //int intzcode = Convert.ToInt32(zonecode);
                                    //cr = generateControl(loginuser, loginpass, RemoteBranch, type, OperatorID, intzcode, "00");

                                    command.CommandText = "update kpforms.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                                    command.Parameters.AddWithValue("st", "00");
                                    command.Parameters.AddWithValue("bcode", RemoteBranch);
                                    command.Parameters.AddWithValue("series", sr + 1);
                                    command.Parameters.AddWithValue("zcode", zonecode);
                                    command.Parameters.AddWithValue("tp", type);
                                    int x = command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpforms.control: "
                                   + " st: " + "00"
                                   + " bcode: " + RemoteBranch
                                   + " nseries: " + (sr + 1)
                                   + " zcode: " + zonecode
                                   + " type: " + type);
                                    if (x < 1)
                                    {
                                        kplog.Error("Review Parameters");
                                        trans.Rollback();
                                        dbconDomestic.CloseConnection();

                                        return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                                    }
                                }
                                else
                                {
                                    //int intzcode = Convert.ToInt32(zonecode);
                                    //cr = generateControl(loginuser, loginpass, bcode, type, OperatorID, intzcode, station);
                                    command.CommandText = "update kpforms.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                                    command.Parameters.AddWithValue("st", station);
                                    command.Parameters.AddWithValue("bcode", bcode);
                                    command.Parameters.AddWithValue("series", sr + 1);
                                    command.Parameters.AddWithValue("zcode", zonecode);
                                    command.Parameters.AddWithValue("tp", type);
                                    command.ExecuteNonQuery();
                                    int x = command.ExecuteNonQuery();
                                    kplog.Info("SUCCESS:: message: update kpforms.control: "
                                    + " station: " + station
                                    + " bcode: " + bcode
                                    + " series: " + (sr + 1)
                                    + " zcode: " + zonecode
                                    + " type: " + type);
                                    if (x < 1)
                                    {
                                        kplog.Error("FAILED:: respcode: 12, message:" + getRespMessage(12) + ",ErrorDetail: Review Parameters");
                                        trans.Rollback();
                                        dbconDomestic.CloseConnection();

                                        return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                                    }
                                }

                                trans.Commit();

                                conn.Close();
                                kplog.Error("FAILED:: respcode: 13, message: Problem saving transaction. Please close the sendout window and open again");
                                return new SendoutResponse { respcode = 13, message = "Problem saving transaction. Please close the sendout window and open again. Thank you.", ErrorDetail = "Review paramerters." };
                            }
                            else
                            {

                                if (myyyx.Number == 1213)
                                {
                                    kplog.Fatal("mysql errcode: 1213", myyyx);
                                    trans.Rollback();
                                    dbconDomestic.CloseConnection();
                                    kplog.Error("FAILED:: respcode: 11, message: " + getRespMessage(11) + ", ErrorDetail: Problem occured during saving. Please resave the transaction");
                                    return new SendoutResponse { respcode = 11, message = getRespMessage(11), ErrorDetail = "Problem occured during saving. Please resave the transaction." };
                                }
                                else
                                {
                                    trans.Rollback();
                                    dbconDomestic.CloseConnection();
                                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myyyx.ToString());
                                    return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = myyyx.ToString() };
                                }
                            }
                        }

                        //command.CommandText = "Insert into " + generateTableName(2, null) + "(`KPTN6`, `MLKP4TN`, `TransDate`, `IsClaimed`, `IsCancelled`) values (@KPTN6, @MLKP4TN, @TransDate1, @IsClaimed1, @IsCancelled1)";
                        //command.Parameters.AddWithValue("KPTN6", kptn);
                        //command.Parameters.AddWithValue("MLKP4TN", kptn4);
                        //command.Parameters.AddWithValue("TransDate1", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        //command.Parameters.AddWithValue("IsClaimed1", 0);
                        //command.Parameters.AddWithValue("IsCancelled1", 0);
                        //command.ExecuteNonQuery();

                        //ControlResponse cr;

                        if (IsRemote.Equals("1"))
                        {
                            //throw new Exception("boom");
                            //int intzcode = Convert.ToInt32(zonecode);
                            //cr = generateControl(loginuser, loginpass, RemoteBranch, type, OperatorID, intzcode, "00");

                            command.CommandText = "update kpforms.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", "00");
                            command.Parameters.AddWithValue("bcode", RemoteBranch);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", remotezcode);
                            command.Parameters.AddWithValue("tp", type);
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpforms.control: "
                                   + " st: " + "00"
                                   + " bcode: " + RemoteBranch
                                   + " nseries: " + (sr + 1)
                                   + " zcode: " + remotezcode
                                   + " type: " + type);
                            if (x < 1)
                            {
                                kplog.Error("FAILED:: repscode: 12, message: " + getRespMessage(12) + ", Error Detail: Review Parameters");
                                trans.Rollback();
                                dbconDomestic.CloseConnection();

                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                            }
                        }
                        else
                        {
                            //int intzcode = Convert.ToInt32(zonecode);
                            //cr = generateControl(loginuser, loginpass, bcode, type, OperatorID, intzcode, station);
                            command.CommandText = "update kpforms.control set nseries = @series where bcode = @bcode and station = @st and zcode = @zcode and type = @tp";
                            command.Parameters.AddWithValue("st", station);
                            command.Parameters.AddWithValue("bcode", bcode);
                            command.Parameters.AddWithValue("series", sr + 1);
                            command.Parameters.AddWithValue("zcode", zonecode);
                            command.Parameters.AddWithValue("tp", type);
                            command.ExecuteNonQuery();
                            int x = command.ExecuteNonQuery();
                            kplog.Info("SUCCESS:: message: update kpforms.global : "
                                + " st: " + station
                                  + " bcode: " + bcode
                                  + " nseries: " + (sr + 1)
                                  + " zcode: " + zonecode
                                  + " type: " + type);
                            if (x < 1)
                            {
                                kplog.Error("FAILED:: respcode: 12, message: " + getRespMessage(12) + ", ERrorDetail: Review Parameters");
                                trans.Rollback();
                                dbconDomestic.CloseConnection();

                                return new SendoutResponse { respcode = 12, message = getRespMessage(12), ErrorDetail = "Review paramerters." };
                            }
                        }


                        if (IsRemote.Equals("1"))
                        {
                            //kptn = validateGeneratedKPTN(RemoteBranch, zonecode, String.Empty);
                            updateResiboDomestic(RemoteBranch, remotezcode, orno, ref command);
                        }
                        else
                        {
                            //kptn = validateGeneratedKPTN(bcode, zonecode, String.Empty);
                            updateResiboDomestic(bcode, zonecode, orno, ref command);
                        }


                        command.Parameters.Clear();
                        command.CommandText = "kpadminlogs.savelog53";
                        command.CommandType = CommandType.StoredProcedure;

                        command.Parameters.AddWithValue("kptnno", kptn);
                        command.Parameters.AddWithValue("action", "SENDOUT");
                        command.Parameters.AddWithValue("isremote", IsRemote);
                        command.Parameters.AddWithValue("txndate", dt);
                        command.Parameters.AddWithValue("stationcode", stationcode);
                        command.Parameters.AddWithValue("stationno", station);
                        command.Parameters.AddWithValue("zonecode", zonecode);
                        command.Parameters.AddWithValue("branchcode", bcode);
                        command.Parameters.AddWithValue("branchname", SenderBranchID);
                        command.Parameters.AddWithValue("operatorid", OperatorID);
                        command.Parameters.AddWithValue("cancelledreason", DBNull.Value);
                        command.Parameters.AddWithValue("remotereason", RemoteReason);
                        command.Parameters.AddWithValue("remotebranch", (RemoteBranchCode.Equals(DBNull.Value)) ? null : RemoteBranchCode);
                        command.Parameters.AddWithValue("remoteoperator", (RemoteOperatorID.Equals(DBNull.Value)) ? null : RemoteOperatorID);
                        command.Parameters.AddWithValue("oldkptnno", DBNull.Value);
                        command.Parameters.AddWithValue("remotezonecode", remotezcode);
                        command.Parameters.AddWithValue("type", "N");

                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message: stored proc: kpadminlogs.savelog53:  "

                       + " kptnno: " + kptn
                        + " action: " + "SENDOUT"
                        + " isremote: " + IsRemote
                        + " txndate: " + dt
                        + " stationcode: " + stationcode
                        + " stationno: " + station
                        + " zonecode: " + zonecode
                        + " branchcode: " + bcode
                        + " branchname: " + SenderBranchID
                        + " operatorid: " + OperatorID
                        + " cancelledreason: " + DBNull.Value
                        + " remotereason: " + RemoteReason
                        + " remotebranch: " + DBNull.Value
                        + " remoteoperator: " + DBNull.Value
                        + " oldkptnno: " + DBNull.Value
                        + " remotezonecode: " + remotezcode
                        + " type: " + "N");


                        trans.Commit();
                        //custtrans.Commit();

                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1) + ", kptn: " + kplog + ", orno: " + orno + ", transdatE:" + dt);
                        return new SendoutResponse { respcode = 1, message = getRespMessage(1), kptn = kptn, orno = orno, transdate = dt };
                    }
                }
                catch (MySqlException myx)
                {
                    kplog.Fatal(myx.ToString());
                    if (myx.Number == 1213)
                    {
                        kplog.Fatal("mysql errcode: 1213");
                        trans.Rollback();
                        dbconDomestic.CloseConnection();
                        kplog.Error("FAILED:: message: respcode: 11, message: " + getRespMessage(11) + ", ErrorDetail: Problem occured during saving. Please resave the transaction.");
                        return new SendoutResponse { respcode = 11, message = getRespMessage(11), ErrorDetail = "Problem occured during saving. Please resave the transaction." };
                    }
                    else
                    {
                        trans.Rollback();
                        dbconDomestic.CloseConnection();
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + myx.ToString());
                        return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = myx.ToString() };
                    }
                }
                catch (Exception ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    trans.Rollback();
                    dbconDomestic.CloseConnection();

                    return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                }
            }

            //using (MySqlConnection conn = dbcon.getConnection())
            //{

            //}
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            //trans.Rollback();
            dbconDomestic.CloseConnection();
            return new SendoutResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }

    }



    private DateTime getCustServerDateGlobal(Boolean isOpenConnection)
    {
        try
        {
            if (!isOpenConnection)
            {
                using (MySqlConnection conn = custconGlobal.getConnection())
                {
                    conn.Open();
                    using (MySqlCommand command = conn.CreateCommand())
                    {

                        DateTime serverdate;

                        command.CommandText = "Select NOW() as serverdt;";
                        using (MySqlDataReader Reader = command.ExecuteReader())
                        {
                            Reader.Read();
                            serverdate = Convert.ToDateTime(Reader["serverdt"]);
                            Reader.Close();
                            conn.Close();

                            return serverdate;
                        }

                    }
                }
            }
            else
            {

                DateTime serverdate;

                command.CommandText = "Select NOW() as serverdt;";

                using (MySqlDataReader Reader = command.ExecuteReader())
                {
                    Reader.Read();
                    serverdate = (DateTime)Reader["serverdt"];
                    Reader.Close();
                    return serverdate;
                }


            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.Message);
        }
    }

    private DateTime getCustServerDateDomestic(Boolean isOpenConnection)
    {
        try
        {
            if (!isOpenConnection)
            {
                using (MySqlConnection conn = custconDomestic.getConnection())
                {
                    conn.Open();
                    using (MySqlCommand command = conn.CreateCommand())
                    {

                        DateTime serverdate;

                        command.CommandText = "Select NOW() as serverdt;";
                        using (MySqlDataReader Reader = command.ExecuteReader())
                        {
                            Reader.Read();
                            serverdate = Convert.ToDateTime(Reader["serverdt"]);
                            Reader.Close();
                            conn.Close();

                            return serverdate;
                        }

                    }
                }
            }
            else
            {

                DateTime serverdate;

                command.CommandText = "Select NOW() as serverdt;";

                using (MySqlDataReader Reader = command.ExecuteReader())
                {
                    Reader.Read();
                    serverdate = (DateTime)Reader["serverdt"];
                    Reader.Close();
                    return serverdate;
                }


            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.Message);
        }
    }


    private DateTime getServerDateDomestic(Boolean isOpenConnection)
    {

        try
        {
            //throw new Exception(isOpenConnection.ToString());
            if (!isOpenConnection)
            {
                using (MySqlConnection conn = dbconDomestic.getConnection())
                {
                    conn.Open();
                    using (MySqlCommand command = conn.CreateCommand())
                    {

                        DateTime serverdate;

                        command.CommandText = "Select NOW() as serverdt;";
                        using (MySqlDataReader Reader = command.ExecuteReader())
                        {
                            Reader.Read();

                            serverdate = Convert.ToDateTime(Reader["serverdt"]);
                            Reader.Close();
                            conn.Close();

                            return serverdate;
                        }

                    }
                }
            }
            else
            {

                DateTime serverdate;

                command.CommandText = "Select NOW() as serverdt;";

                using (MySqlDataReader Reader = command.ExecuteReader())
                {
                    Reader.Read();
                    serverdate = Convert.ToDateTime(Reader["serverdt"]);
                    Reader.Close();
                    return serverdate;
                }


            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.Message);
        }
    }

    private DateTime getServerDateGlobal(Boolean isOpenConnection)
    {

        try
        {
            //throw new Exception(isOpenConnection.ToString());
            if (!isOpenConnection)
            {
                using (MySqlConnection conn = dbconGlobal.getConnection())
                {
                    conn.Open();
                    using (MySqlCommand command = conn.CreateCommand())
                    {

                        DateTime serverdate;

                        command.CommandText = "Select NOW() as serverdt;";
                        using (MySqlDataReader Reader = command.ExecuteReader())
                        {
                            Reader.Read();

                            serverdate = Convert.ToDateTime(Reader["serverdt"]);
                            Reader.Close();
                            conn.Close();

                            return serverdate;
                        }

                    }
                }
            }
            else
            {

                DateTime serverdate;

                command.CommandText = "Select NOW() as serverdt;";

                using (MySqlDataReader Reader = command.ExecuteReader())
                {
                    Reader.Read();
                    serverdate = Convert.ToDateTime(Reader["serverdt"]);
                    Reader.Close();
                    return serverdate;
                }


            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.Message);
        }
    }



    private DateTime getServerDateGlobal(Boolean isOpenConnection, MySqlCommand mycommand)
    {

        try
        {
            //throw new Exception(isOpenConnection.ToString());
            if (!isOpenConnection)
            {
                using (MySqlConnection conn = dbconGlobal.getConnection())
                {
                    conn.Open();
                    using (MySqlCommand command = conn.CreateCommand())
                    {

                        DateTime serverdate;

                        command.CommandText = "Select NOW() as serverdt;";
                        using (MySqlDataReader Reader = command.ExecuteReader())
                        {
                            Reader.Read();

                            serverdate = Convert.ToDateTime(Reader["serverdt"]);
                            Reader.Close();
                            conn.Close();

                            return serverdate;
                        }

                    }
                }
            }
            else
            {

                DateTime serverdate;

                mycommand.CommandText = "Select NOW() as serverdt;";

                using (MySqlDataReader Reader = mycommand.ExecuteReader())
                {
                    Reader.Read();
                    serverdate = Convert.ToDateTime(Reader["serverdt"]);
                    Reader.Close();
                    return serverdate;
                }


            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.Message);
        }
    }



    [WebMethod]
    public String testGenerateTable(Int32 type)
    {
        kplog.Info("type: "+type);

        dt = getServerDateGlobal(false);
        return generateTableNameGlobal(type, null);
    }

    private String generateTableNameGlobalMobile(Int32 type, String TransDate)
    {
        //DateTime dt = getServerDate(false);

        if (TransDate == null)
        {
            if (type == 0)
            {
                return (isUse365Global == 0) ? "Mobexglobal.sendout" : "Mobexglobal.sendout" + dt.ToString("MM") + dt.ToString("dd");
            }
            else if (type == 1)
            {
                return (isUse365Global == 0) ? "Mobexglobal.payout" : "Mobexglobal.payout" + dt.ToString("MM") + dt.ToString("dd");
            }
            else if (type == 2)
            {
                return (isUse365Global == 0) ? "Mobexglobal.tempkptn" : "Mobexglobal.tempkptn";
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid transaction type");
                throw new Exception("Invalid transaction type");
            }
        }
        else
        {
            DateTime TransDatetoDate = Convert.ToDateTime(TransDate);
            if (type == 0)
            {
                return (isUse365Global == 0) ? "Mobexglobal.sendout" : "Mobexglobal.sendout" + TransDatetoDate.ToString("MM") + TransDatetoDate.ToString("dd");
            }
            else if (type == 1)
            {
                return (isUse365Global == 0) ? "Mobexglobal.payout" : "Mobexglobal.payout" + TransDatetoDate.ToString("MM") + TransDatetoDate.ToString("dd");
            }
            else if (type == 2)
            {
                return (isUse365Global == 0) ? "Mobexglobal.tempkptn" : "Mobexglobal.tempkptn";
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid transaction type");
                throw new Exception("Invalid transaction type");
            }
        }
    }

    private String generateTableNameGlobal(Int32 type, String TransDate)
    {

        //DateTime dt = getServerDate(false);

        if (TransDate == null)
        {
            if (type == 0)
            {
                return (isUse365Global == 0) ? "kpglobal.sendout" : "kpglobal.sendout" + dt.ToString("MM") + dt.ToString("dd");
            }
            else if (type == 1)
            {
                return (isUse365Global == 0) ? "kpglobal.payout" : "kpglobal.payout" + dt.ToString("MM") + dt.ToString("dd");
            }
            else if (type == 2)
            {
                return (isUse365Global == 0) ? "kpglobal.tempkptn" : "kpglobal.tempkptn";
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid transaction type");
                throw new Exception("Invalid transaction type");
            }
        }
        else
        {
            DateTime TransDatetoDate = Convert.ToDateTime(TransDate);
            if (type == 0)
            {
                return (isUse365Global == 0) ? "kpglobal.sendout" : "kpglobal.sendout" + TransDatetoDate.ToString("MM") + TransDatetoDate.ToString("dd");
            }
            else if (type == 1)
            {
                return (isUse365Global == 0) ? "kpglobal.payout" : "kpglobal.payout" + TransDatetoDate.ToString("MM") + TransDatetoDate.ToString("dd");
            }
            else if (type == 2)
            {
                return (isUse365Global == 0) ? "kpglobal.tempkptn" : "kpglobal.tempkptn";
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid transaction type");
                throw new Exception("Invalid transaction type");
            }
        }
    }

    private String generateTableNameDomestic(Int32 type, String TransDate)
    {
        //DateTime dt = getServerDate(false);

        if (TransDate == null)
        {
            if (type == 0)
            {
                return (isUse365Domestic == 0) ? "kpdomestic.sendout" : "kpdomestic.sendout" + dt.ToString("MM") + dt.ToString("dd");
            }
            else if (type == 1)
            {
                return (isUse365Domestic == 0) ? "kpdomestic.payout" : "kpdomestic.payout" + dt.ToString("MM") + dt.ToString("dd");
            }
            else if (type == 2)
            {
                return (isUse365Domestic == 0) ? "kpdomestic.tempkptn" : "kpdomestic.tempkptn";
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid transaction type");
                throw new Exception("Invalid transaction type");
            }
        }
        else
        {
            DateTime TransDatetoDate = Convert.ToDateTime(TransDate);
            if (type == 0)
            {
                return (isUse365Domestic == 0) ? "kpdomestic.sendout" : "kpdomestic.sendout" + TransDatetoDate.ToString("MM") + TransDatetoDate.ToString("dd");
            }
            else if (type == 1)
            {
                return (isUse365Domestic == 0) ? "kpdomestic.payout" : "kpdomestic.payout" + TransDatetoDate.ToString("MM") + TransDatetoDate.ToString("dd");
            }
            else if (type == 2)
            {
                return (isUse365Domestic == 0) ? "kpdomestic.tempkptn" : "kpdomestic.tempkptn";
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid transaction type");
                throw new Exception("Invalid transaction type");
            }
        }
    }


    private Boolean validateKPTN4(String kptn4)
    {
        try
        {
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                using (MySqlCommand command = conn.CreateCommand())
                {
                    conn.Open();
                    //DateTime dt;
                    command.CommandText = "select MLKP4TN from " + generateTableNameGlobal(2, null) + " where MLKP4TN = @kptn4;";
                    command.Parameters.AddWithValue("kptn4", kptn4);
                    using (MySqlDataReader Reader = command.ExecuteReader())
                    {

                        if (Reader.Read())
                        {
                            Reader.Close();
                            conn.Close();
                            return true;
                        }
                        else
                        {
                            Reader.Close();
                            conn.Close();
                            return false;
                        }
                    }

                }
            }

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.Message);
        }
    }

    public String getcustomertable(String lastname)
    {
        kplog.Info("lastname: "+lastname);

        String customers = "";
        lastname.ToUpper();
        if (lastname.StartsWith("A") || lastname.StartsWith("B") || lastname.StartsWith("C"))
        {
            customers = "AtoC";
        }
        else if (lastname.StartsWith("D") || lastname.StartsWith("E") || lastname.StartsWith("F"))
        {
            customers = "DtoF";
        }
        else if (lastname.StartsWith("G") || lastname.StartsWith("H") || lastname.StartsWith("I"))
        {
            customers = "GtoI";
        }
        else if (lastname.StartsWith("J") || lastname.StartsWith("K") || lastname.StartsWith("L"))
        {
            customers = "JtoL";
        }
        else if (lastname.StartsWith("M") || lastname.StartsWith("N") || lastname.StartsWith("O"))
        {
            customers = "MtoO";
        }
        else if (lastname.StartsWith("P") || lastname.StartsWith("Q") || lastname.StartsWith("R"))
        {
            customers = "PtoR";
        }
        else if (lastname.StartsWith("S") || lastname.StartsWith("T") || lastname.StartsWith("U"))
        {
            customers = "StoU";
        }
        else if (lastname.StartsWith("V") || lastname.StartsWith("W") || lastname.StartsWith("X"))
        {
            customers = "VtoX";
        }
        else if (lastname.StartsWith("Y") || lastname.StartsWith("Z"))
        {
            customers = "YtoZ";
        }
        kplog.Info("SUCCESS:: message: " + customers);
        return customers;
    }

    [WebMethod]
    public Boolean testValidatekptn4(String kptn4)
    {
        kplog.Info("kptn4: "+kptn4);

        kplog.Info("SUCCESS::message: " + kptn4);
        return validateKPTN4(kptn4);
    }



    private Decimal CalculateDormantChargeGlobal(DateTime SODate)
    {

        try
        {
            //conn.Open();
            //using (command = conn.CreateCommand())
            //{
            String queryDormant = "SELECT " +
                                  "IF(@SODate > DATE_SUB(NOW(), " +
                                  "INTERVAL (30 * (SELECT ChargeMonth " +
                                                   "FROM kpformsglobal.syscharges " +
                                                   "WHERE ChargeCode = 'Dormant')) DAY), " +
                                                   "0, " +
                                                   "(SELECT ROUND(" +
                                                        "DATEDIFF(DATE_SUB(NOW(), INTERVAL (30 * (SELECT ChargeMonth FROM kpformsglobal.syscharges WHERE ChargeCode = 'Dormant')) DAY),@SODate) / 30,0) * (SELECT ChargeAmount FROM kpformsglobal.syscharges WHERE ChargeCode = 'Dormant')))  AS charge; ";
            command.CommandText = queryDormant;
            command.Parameters.AddWithValue("SODate", SODate.ToString("yyyy-MM-dd HH:mm:ss"));
            MySqlDataReader ReaderDormant = command.ExecuteReader();
            Decimal ChargeAmount;
            if (ReaderDormant.HasRows)
            {
                ReaderDormant.Read();

                ChargeAmount = ReaderDormant["charge"].Equals(DBNull.Value) ? 0 : (Decimal)ReaderDormant["charge"];
                ReaderDormant.Close();
                return ChargeAmount;
            }
            else
            {

                ChargeAmount = 0;
                ReaderDormant.Close();
                return ChargeAmount;
            }
        }
        //}
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }


    private Decimal CalculateDormantChargeDomestic(DateTime SODate)
    {

        try
        {
            //conn.Open();
            //using (command = conn.CreateCommand())
            //{
            String queryDormant = "SELECT " +
                                  "IF(@SODate > DATE_SUB(NOW(), " +
                                  "INTERVAL (30 * (SELECT ChargeMonth " +
                                                   "FROM kpforms.syscharges " +
                                                   "WHERE ChargeCode = 'Dormant')) DAY), " +
                                                   "0, " +
                                                   "(SELECT ROUND(" +
                                                        "DATEDIFF(DATE_SUB(NOW(), INTERVAL (30 * (SELECT ChargeMonth FROM kpforms.syscharges WHERE ChargeCode = 'Dormant')) DAY),@SODate) / 30,0) * (SELECT ChargeAmount FROM kpforms.syscharges WHERE ChargeCode = 'Dormant')))  AS charge; ";
            command.CommandText = queryDormant;
            command.Parameters.AddWithValue("SODate", SODate.ToString("yyyy-MM-dd HH:mm:ss"));
            MySqlDataReader ReaderDormant = command.ExecuteReader();
            Decimal ChargeAmount;
            if (ReaderDormant.HasRows)
            {
                ReaderDormant.Read();

                ChargeAmount = ReaderDormant["charge"].Equals(DBNull.Value) ? 0 : (Decimal)ReaderDormant["charge"];
                ReaderDormant.Close();
                return ChargeAmount;
            }
            else
            {
                ChargeAmount = 0;
                ReaderDormant.Close();
                return ChargeAmount;
            }
        }
        //}
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }


    [WebMethod]
    public String duplicateTest(string d)
    {
        //String guid = Guid.NewGuid().GetHashCode().ToString();
        //jp.takel.PseudoRandom.MersenneTwister a = new jp.takel.PseudoRandom.MersenneTwister(Convert.ToUInt32(Convert.ToInt32(guid).GetHashCode()));
        jp.takel.PseudoRandom.MersenneTwister rand = new jp.takel.PseudoRandom.MersenneTwister((uint)DateTime.Now.Ticks);
        //list<int32> atay = new list<int32>();
        //for (int i = 0; i < 10000; ++i)
        //{
        //    atay.add(randgen.next(1000000000, int.maxvalue));
        //    //if (i % 5 == 4) console.writeline("");
        //}
        return rand.Next(1000000000, int.MaxValue).ToString();
        //return a.Next().ToString();
        //return generateKPTNGlobal("004",1);
        //int shorthash = "test".GetHashCode() % 10000000; // 8 zeros
        //if (shorthash < 0) shorthash *= -1;
        //return generateKPTN("023","2","asdf");
        //String[] a = new String[1000];
        //for (int x = 0; x < 1000;x++){
        //    a[x] = validateGeneratedKPTN("356", "1", String.Empty);
        //}
        //return a;
        //return decodeKPTN(0, "004091727258269705");
        //if (d.Contains("duplicate"))
        //{
        //    return "yes";
        //}
        //else
        //{
        //    return "no";
        //}

    }


    public String getRespMessage(Int32 code)
    {
        String x = "SYSTEM_ERROR";
        switch (code)
        {
            case 1:
                return x = "Success";
            case 2:
                return x = "Duplicate kptn";
            case 3:
                return x = "KPTN already claimed";
            case 4:
                return x = "KPTN not found";
            case 5:
                return x = "Customer not found";
            case 6:
                return x = "Customer already exist";
            case 7:
                return x = "Invalid credentials";
            case 8:
                return x = "KPTN already cancelled";
            case 9:
                return x = "Transaction is not yet claimed";
            case 10:
                return x = "Version does not match";
            case 11:
                return x = "Problem occured during saving. Please resave the transaction.";
            case 12:
                return x = "Problem saving transaction. Please close the sendout form and open it again. Thank you.";
            case 13:
                return x = "Invalid station number.";
            case 14:
                return x = "Error generating receipt number.";
            case 15:
                return x = "Unable to save transaction. Invalid amount provided.";
            case 16:
                return x = "Branch does not exist in Branch Charges.";
            default:
                return x;
        }
    }

    [WebMethod]
    public string testini()
    {
        String a = Server.MapPath("boskpws.ini");
        IniFile ini = new IniFile(a);
        String aaa = ini.IniReadValue("DBConfig", "SERVER");
        return aaa;
    }

    [WebMethod]
    public String[] testDir()
    {
        DirectoryInfo directoryInfo = new DirectoryInfo(Server.MapPath("app_code/utils"));
        FileInfo[] rgFiles = directoryInfo.GetFiles("*");
        string[] a = new string[rgFiles.Length];

        int x = 0;
        foreach (FileInfo fi in rgFiles)
        {
            a[x] = fi.Name;
            x++;
        }
        return a;
    }

    private Boolean authenticate(String username, String password)
    {
        if (loginuser.Equals(username) && loginpass.Equals(password))
        {
            return true;
        }
        else
        {
            kplog.Error("FAILED:: message: Invalid credentials");
            return false;
        }
    }


    private void ConnectGlobal()
    {
        try
        {
            //string path = httpcontext.current.server.mappath("boskpws.ini");
            IniFile ini = new IniFile(pathGlobal);


            String Serv = ini.IniReadValue("DBConfig Transaction", "server");
            String DB = ini.IniReadValue("DBConfig Transaction", "database"); ;
            String UID = ini.IniReadValue("DBConfig Transaction", "uid"); ;
            String Password = ini.IniReadValue("DBConfig Transaction", "password"); ;
            String pool = ini.IniReadValue("DBConfig Transaction", "pool");
            Int32 maxcon = Convert.ToInt32(ini.IniReadValue("DBConfig Transaction", "maxcon"));
            Int32 mincon = Convert.ToInt32(ini.IniReadValue("DBConfig Transaction", "mincon"));
            Int32 tout = Convert.ToInt32(ini.IniReadValue("DBConfig Transaction", "tout"));
            dbconGlobal = new DBConnect(Serv, DB, UID, Password, pool, maxcon, mincon, tout);


            String CustServ = ini.IniReadValue("DBConfig Customer", "Server");
            String CustDB = ini.IniReadValue("DBConfig Customer", "Database"); ;
            String CustUID = ini.IniReadValue("DBConfig Customer", "UID"); ;
            String CustPassword = ini.IniReadValue("DBConfig Customer", "Password"); ;
            String Custpool = ini.IniReadValue("DBConfig Customer", "Pool");
            Int32 Custmaxcon = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "MaxCon"));
            Int32 Custmincon = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "MinCon"));
            Int32 Custtout = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "Tout"));
            custconGlobal = new DBConnect(CustServ, CustDB, CustUID, CustPassword, Custpool, Custmaxcon, Custmincon, Custtout);


            String cmmsServ = ini.IniReadValue("DBConfig CMMS", "server");
            String cmmsDB = ini.IniReadValue("DBConfig CMMS", "database"); ;
            String cmmsUID = ini.IniReadValue("DBConfig CMMS", "uid"); ;
            String cmmsPassword = ini.IniReadValue("DBConfig CMMS", "password"); ;
            String cmmspool = ini.IniReadValue("DBConfig CMMS", "pool");
            Int32 cmmsmaxcon = Convert.ToInt32(ini.IniReadValue("DBConfig CMMS", "maxcon"));
            Int32 cmmsmincon = Convert.ToInt32(ini.IniReadValue("DBConfig CMMS", "mincon"));
            Int32 cmmstout = Convert.ToInt32(ini.IniReadValue("DBConfig CMMS", "tout"));
            CMMSConnectGlobal = new DBConnect(cmmsServ, cmmsDB, cmmsUID, cmmsPassword, cmmspool, cmmsmaxcon, cmmsmincon, cmmstout);
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }


    private void ConnectDomestic()
    {
        try
        {
            //string path = httpcontext.current.server.mappath("boskpws.ini");
            IniFile ini = new IniFile(pathDomestic);


            String Serv = ini.IniReadValue("DBConfig Transaction", "server");
            String DB = ini.IniReadValue("DBConfig Transaction", "database"); ;
            String UID = ini.IniReadValue("DBConfig Transaction", "uid"); ;
            String Password = ini.IniReadValue("DBConfig Transaction", "password"); ;
            String pool = ini.IniReadValue("DBConfig Transaction", "pool");
            Int32 maxcon = Convert.ToInt32(ini.IniReadValue("DBConfig Transaction", "maxcon"));
            Int32 mincon = Convert.ToInt32(ini.IniReadValue("DBConfig Transaction", "mincon"));
            Int32 tout = Convert.ToInt32(ini.IniReadValue("DBConfig Transaction", "tout"));
            dbconDomestic = new DBConnect(Serv, DB, UID, Password, pool, maxcon, mincon, tout);


            String CustServ = ini.IniReadValue("DBConfig Customer", "Server");
            String CustDB = ini.IniReadValue("DBConfig Customer", "Database"); ;
            String CustUID = ini.IniReadValue("DBConfig Customer", "UID"); ;
            String CustPassword = ini.IniReadValue("DBConfig Customer", "Password"); ;
            String Custpool = ini.IniReadValue("DBConfig Customer", "Pool");
            Int32 Custmaxcon = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "MaxCon"));
            Int32 Custmincon = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "MinCon"));
            Int32 Custtout = Convert.ToInt32(ini.IniReadValue("DBConfig Customer", "Tout"));
            custconDomestic = new DBConnect(CustServ, CustDB, CustUID, CustPassword, Custpool, Custmaxcon, Custmincon, Custtout);


            String cmmsServ = ini.IniReadValue("DBConfig CMMS", "server");
            String cmmsDB = ini.IniReadValue("DBConfig CMMS", "database"); ;
            String cmmsUID = ini.IniReadValue("DBConfig CMMS", "uid"); ;
            String cmmsPassword = ini.IniReadValue("DBConfig CMMS", "password"); ;
            String cmmspool = ini.IniReadValue("DBConfig CMMS", "pool");
            Int32 cmmsmaxcon = Convert.ToInt32(ini.IniReadValue("DBConfig CMMS", "maxcon"));
            Int32 cmmsmincon = Convert.ToInt32(ini.IniReadValue("DBConfig CMMS", "mincon"));
            Int32 cmmstout = Convert.ToInt32(ini.IniReadValue("DBConfig CMMS", "tout"));
            CMMSConnectDomestic = new DBConnect(cmmsServ, cmmsDB, cmmsUID, cmmsPassword, cmmspool, cmmsmaxcon, cmmsmincon, cmmstout);
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    private Boolean verifyCustomer(String fname, String lname, String mname, String bdate)
    {
        try
        {
            using (MySqlConnection custconn = custconGlobal.getConnection())
            {
                try
                {

                    custconn.Open();
                    using (MySqlCommand custcommand = custconn.CreateCommand())
                    {
                        custcommand.CommandText = "Select FirstName from kpcustomersglobal.customers where FirstName = @fname and LastName = @lname and MiddleName = @mname and BirthDate = @bdate LIMIT 1";
                        custcommand.Parameters.AddWithValue("fname", fname);
                        custcommand.Parameters.AddWithValue("lname", lname);
                        custcommand.Parameters.AddWithValue("mname", mname);
                        custcommand.Parameters.AddWithValue("bdate", bdate);

                        using (MySqlDataReader Reader = custcommand.ExecuteReader())
                        {
                            Reader.Read();
                            if (Reader.HasRows)
                            {
                                Reader.Close();
                                custconn.Close();
                                //throw new Exception("asdf");
                                return true;
                            }
                            else
                            {

                                Reader.Close();
                                custconn.Close();
                                return false;
                            }
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                    custconn.Close();
                    return false;
                }
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            return false;
        }

    }

    private Boolean compareVersions(Double versionHO, Double versionDev)
    {
        //throw new Exception(versionHO.ToString() + " " + versionDev.ToString());
        if (versionHO == versionDev)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    private Double getVersion(String stationcode)
    {
        using (MySqlConnection conn = CMMSConnectGlobal.getConnection())
        {
            try
            {
                conn.Open();
                using (MySqlCommand command = conn.CreateCommand())
                {
                    String getVersion = "SELECT version FROM kpusersglobal.mlbranchesstations where StationCode = @stationcode LIMIT 1";
                    command.CommandText = getVersion;
                    command.Parameters.AddWithValue("stationcode", stationcode);
                    using (MySqlDataReader ReaderVersion = command.ExecuteReader())
                    {
                        ReaderVersion.Read();
                        Double version = Convert.ToDouble(ReaderVersion["version"]);
                        ReaderVersion.Close();
                        conn.Close();

                        return version;
                    }
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                conn.Close();
                return 0.0;
            }
        }
    }

    private String decodeKPTNGlobald2b(int type, String kptn)
    {
        try
        {

            String month = kptn.Substring(kptn.Length - 2, 2);
            String day = kptn.Substring(6, 2);
            int x = Convert.ToInt32(month);
            int y = Convert.ToInt32(day);
            if (type == 0)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {

                    return "kpglobal.sendoutd2b" + month + day;
                }

            }
            else if (type == 1)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {
                    return "kpglobal.payoutd2b" + month + day;
                }
            }
            else
            {
                kplog.Error("FAILED:: message: invalid type!");
                throw new Exception("invalid type");
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception("4");
        }
    }

    private String decodeKPTNGlobalMobile(int type, String kptn)
    {
        try
        {
            //String chkkptn = kptn.Substring(0, 4);
            String month = kptn.Substring(kptn.Length - 2, 2);
            String day = kptn.Substring(6, 2);
            //String day = kptn.Substring(chkkptn == "MLG" ? 6 : 7, 2);
            int x = Convert.ToInt32(month);
            int y = Convert.ToInt32(day);
            if (type == 0)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {

                    return "Mobexglobal.sendout" + month + day;
                }

            }
            else if (type == 1)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {
                    return "Mobexglobal.payout" + month + day;
                }
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid Type!");
                throw new Exception("invalid type");
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception("4");
        }
    }

    private String decodeKPTNGlobalKioskGlobal(int type, String kptn)
    {
        try
        {
            //String chkkptn = kptn.Substring(0, 4);
            String month = kptn.Substring(kptn.Length - 2, 2);
            String day = kptn.Substring(6, 2);
            //String day = kptn.Substring(chkkptn == "MLG" ? 6 : 7, 2);
            int x = Convert.ToInt32(month);
            int y = Convert.ToInt32(day);
            if (type == 0)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {

                    return "kpkioskglobal.sendout" + month + day;
                }

            }
            else if (type == 1)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {
                    return "kpkioskglobal.payout" + month + day;
                }
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid Type!'");
                throw new Exception("invalid type");
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception("4");
        }
    }

    private String decodeKPTNGlobal(int type, String kptn)
    {
        try
        {

            String month = kptn.Substring(kptn.Length - 2, 2);
            String day = kptn.Substring(6, 2);
            int x = Convert.ToInt32(month);
            int y = Convert.ToInt32(day);
            if (type == 0)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {

                    return "kpglobal.sendout" + month + day;
                }

            }
            else if (type == 1)
            {

                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {
                    return "kpglobal.payout" + month + day;
                }
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid Type!");
                throw new Exception("invalid type");
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception("4");
        }
    }

    private String decodeKPTNDomestic(int type, String kptn)
    {
        try
        {
            if (type == 0)
            {
                int x = Convert.ToInt32(kptn.Substring(kptn.Length - 2, 2));
                int y = Convert.ToInt32(kptn.Substring(3, 2));
                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {
                    return "kpdomestic.sendout" + kptn.Substring(kptn.Length - 2, 2) + kptn.Substring(3, 2);
                }

            }
            else if (type == 1)
            {
                int x = Convert.ToInt32(kptn.Substring(kptn.Length - 2, 2));
                int y = Convert.ToInt32(kptn.Substring(3, 2));
                if (x > 12 || x < 0 || x == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else if (y > 31 || y < 0 || y == 0)
                {
                    kplog.Error("FAILED:: message: Throw '4'");
                    throw new Exception("4");
                }
                else
                {
                    return "kpdomestic.payout" + kptn.Substring(kptn.Length - 2, 2) + kptn.Substring(3, 2);
                }
            }
            else
            {
                kplog.Error("FAILED:: message: Invalid Type!");
                throw new Exception("invalid type");
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception("4");
        }
    }

    [WebMethod(Description = "Check customer in watchlist table.")]
    public SeccomResponse verifyCustomer(String Username, String Password)
    {
        kplog.Info("Username: "+Username+", Password: "+Password);

        if (!authenticate(Username, Password))
        {
            kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
            return new SeccomResponse { respcode = 7, message = getRespMessage(7) };
        }
        return null;
    }


    //public Boolean verifyMLCardNo(String mlcard)
    //{
    //    try
    //    {
    //        using (MySqlConnection custconn = custcon.getConnection())
    //        {
    //            try
    //            {
    //                custconn.Open();
    //                using (custcommand = custconn.CreateCommand())
    //                {
    //                    if (mlcard.Equals(string.Empty)) {
    //                        return true;
    //                    }else{
    //                        command.CommandText = "Select mlcard from kpcustomer.";
    //                    }
    //                }
    //            }
    //            catch (MySqlException ex)
    //            {
    //            }
    //        }
    //    }
    //    catch (Exception ex)
    //    {

    //    }


    //}

    [WebMethod]
    public int newGenerationOFKPTM(String branchcode, String zonecode)
    {
        kplog.Info("branchcode: "+branchcode+", zonecode: "+zonecode);

        Random rand = new Random();
        return rand.Next(10, 99);
        //dt = getServerDate(false);

        //String guid = Guid.NewGuid().GetHashCode().ToString();
        //Random rand = new Random();
        //int shorthash = guid.GetHashCode() % 100000000; // 8 zeros
        //if (shorthash < 0) shorthash *= -1;
        //return shorthash;
        //return branchcode + dt.ToString("dd") + zonecode + rand.Next(10, 99).ToString() + "" + shorthash.ToString() + dt.ToString("MM");;


    }


    private Boolean isSameYear2(DateTime date)
    {
        try
        {
            //throw new Exception(date.Year.ToString());
            if (GetYesterday2(date).Year.Equals(date.Year))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }

    }




    private DateTime GetYesterday2(DateTime date)
    {
        return date.AddDays(-1);
    }


    [WebMethod]
    public Boolean testISyear()
    {
        dt = getServerDateGlobal(false);
        return isSameYear2(dt);
    }

    [WebMethod]
    public PeepLog saveLog(String Username, String Password, String kptnno, String action, Boolean isremote, String stationcode, String stationno, int zonecode, String branchcode, String branchname, String operatorid, Double version, String cancelledreason, String remotereason, String remotebranch, String remoteoperator, String oldkptnno)
    {
        kplog.Info("Username: "+Username+", Password: "+Password+", kptnno: "+kptnno+", action: "+action+", isremote: "+isremote+", stationcode: "+stationcode+", stationno: "+stationno+", zonecode: "+zonecode+", branchcode: "+branchcode+", branchname: "+branchname+", operatorid: "+operatorid+", version: "+version+", cancelledreason: "+cancelledreason+", remotereason: "+remotereason+", remotebranch: "+remotebranch+", remoteoperator: "+remoteoperator+", oldkptnno: "+oldkptnno);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new PeepLog { respcode = 7, message = getRespMessage(7) };
            }
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new PeepLog { respcode = 10, message = getRespMessage(10) };
            //}
            using (MySqlConnection conn = dbconDomestic.getConnection())
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                    command.Transaction = trans;
                    command = new MySqlCommand("kpadminlogs.savelog53", conn);
                    command.CommandType = CommandType.StoredProcedure;

                    command.Parameters.AddWithValue("kptnno", kptnno);
                    command.Parameters.AddWithValue("action", action);
                    command.Parameters.AddWithValue("isremote", isremote);
                    command.Parameters.AddWithValue("txndate", DBNull.Value);
                    command.Parameters.AddWithValue("stationcode", stationcode);
                    command.Parameters.AddWithValue("stationno", stationno);
                    command.Parameters.AddWithValue("zonecode", zonecode);
                    command.Parameters.AddWithValue("branchcode", branchcode);
                    command.Parameters.AddWithValue("branchname", branchname);
                    command.Parameters.AddWithValue("operatorid", operatorid);
                    command.Parameters.AddWithValue("cancelledreason", cancelledreason);
                    command.Parameters.AddWithValue("remotereason", remotereason);
                    command.Parameters.AddWithValue("remotebranch", remotebranch);
                    command.Parameters.AddWithValue("remoteoperator", remoteoperator);
                    command.Parameters.AddWithValue("remotezonecode", DBNull.Value);
                    command.Parameters.AddWithValue("oldkptnno", DBNull.Value);

                    command.Parameters.AddWithValue("type", "N");

                    try
                    {
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message: stored proc: kpadminlogs.savelog53: "
                    + " kptnno: " + kptnno
                    + " action: " + action
                    + " isremote: " + isremote
                    + " txndate: " + DBNull.Value
                    + " stationcode: " + stationcode
                    + " stationno: " + stationno
                    + " zonecode: " + zonecode
                    + " branchcode: " + branchcode
                    + " branchname: " + branchname
                    + " operatorid: " + operatorid
                    + " cancelledreason: " + cancelledreason
                    + " remotereason: " + remotereason
                    + " remotebranch: " + remotebranch
                    + " remoteoperator: " + remoteoperator
                    + " remotezonecode: " + DBNull.Value
                    + " oldkptnno: " + DBNull.Value
                    + " type: " + "N");
                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1));
                        return new PeepLog { respcode = 1, message = getRespMessage(1) };
                    }
                    catch (MySqlException ex)
                    {
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                        trans.Rollback();
                        conn.Close();
                        return new PeepLog { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconDomestic.CloseConnection();
            return new PeepLog { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconDomestic.CloseConnection();
            return new PeepLog { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    [WebMethod]
    public PeepLog saveLogGlobal(String Username, String Password, String kptnno, String action, Boolean isremote, String stationcode, String stationno, int zonecode, String branchcode, String branchname, String operatorid, Double version, String cancelledreason, String remotereason, String remotebranch, String remoteoperator, String oldkptnno)
    {
        kplog.Info("Username: " + Username + ", Password: " + Password + ", kptnno: " + kptnno + ", action: " + action + ", isremote: " + isremote + ", stationcode: " + stationcode + ", stationno: " + stationno + ", zonecode: " + zonecode + ", branchcode: " + branchcode + ", branchname: " + branchname + ", operatorid: " + operatorid + ", version: " + version + ", cancelledreason: " + cancelledreason + ", remotereason: " + remotereason + ", remotebranch: " + remotebranch + ", remoteoperator: " + remoteoperator + ", oldkptnno: " + oldkptnno);

        try
        {
            if (!authenticate(Username, Password))
            {
                kplog.Error("FAILED:: respcode: 7 , message: Invalid Credentials!");
                return new PeepLog { respcode = 7, message = getRespMessage(7) };
            }
            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new PeepLog { respcode = 10, message = getRespMessage(10) };
            //}
            using (MySqlConnection conn = dbconGlobal.getConnection())
            {
                using (command = conn.CreateCommand())
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                    command.Transaction = trans;
                    command = new MySqlCommand("kpadminlogsglobal.savelog53", conn);
                    command.CommandType = CommandType.StoredProcedure;

                    command.Parameters.AddWithValue("kptnno", kptnno);
                    command.Parameters.AddWithValue("action", action);
                    command.Parameters.AddWithValue("isremote", isremote);
                    command.Parameters.AddWithValue("txndate", "now()");
                    command.Parameters.AddWithValue("stationcode", stationcode);
                    command.Parameters.AddWithValue("stationno", stationno);
                    command.Parameters.AddWithValue("zonecode", zonecode);
                    command.Parameters.AddWithValue("branchcode", branchcode);
                    //command.Parameters.AddWithValue("branchname", branchname);
                    command.Parameters.AddWithValue("operatorid", operatorid);
                    command.Parameters.AddWithValue("cancelledreason", cancelledreason);
                    command.Parameters.AddWithValue("remotereason", remotereason);
                    command.Parameters.AddWithValue("remotebranch", remotebranch);
                    command.Parameters.AddWithValue("remoteoperator", remoteoperator);
                    command.Parameters.AddWithValue("remotezonecode", 0);
                    command.Parameters.AddWithValue("oldkptnno", DBNull.Value);

                    command.Parameters.AddWithValue("type", "N");

                    try
                    {
                        command.ExecuteNonQuery();
                        kplog.Info("SUCCESS:: message: stored proc: kpadminlogsglobal.savelog53: "
                    + " kptnno: " + kptnno
                    + " action: " + action
                    + " isremote: " + isremote
                    + " txndate: " + DateTime.Now
                    + " stationcode: " + stationcode
                    + " stationno: " + stationno
                    + " zonecode: " + zonecode
                    + " branchcode: " + branchcode
                    + " operatorid: " + operatorid
                    + " cancelledreason: " + cancelledreason
                    + " remotereason: " + remotereason
                    + " remotebranch: " + remotebranch
                    + " remoteoperator: " + remoteoperator
                    + " remotezonecode: " + 0
                    + " oldkptnno: " + DBNull.Value

                    + " type: " + "N");
                        trans.Commit();
                        conn.Close();
                        kplog.Info("SUCCESS:: respcode: 1 , message: " + getRespMessage(1));
                        return new PeepLog { respcode = 1, message = getRespMessage(1) };
                    }
                    catch (MySqlException ex)
                    {
                        kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
                        trans.Rollback();
                        conn.Close();
                        return new PeepLog { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconGlobal.CloseConnection();
            return new PeepLog { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            dbconGlobal.CloseConnection();
            return new PeepLog { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
        }
    }

    private String validateGeneratedKPTN(String bcode, String zonecode, String initiator)
    {

        try
        {
            String kptn = String.Empty;
            kptn = generateKPTN(bcode, zonecode, initiator);
            while (kptn.Length < 18 || kptn.Length > 18)
            {
                kptn = generateKPTN(bcode, zonecode, String.Empty);

            }
            return kptn;

        }
        catch (Exception ex)
        {
            kplog.Fatal("FAILED:: respcode: 0 message : " + getRespMessage(0) + " , ErrorDetail: " + ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    private Boolean verifyValidity(DateTime now, DateTime end)
    {
        // 0 = equals, -1 = greater , 1 = less than
        int x = now.CompareTo(end);
        if (x >= 0)
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    [WebMethod]
    public Int32 getCustIDCount(String FirstName, String LastName, String MiddleName, String Birthdate)
    {

        int custcounter = 0;
        String query = "SELECT CustID, FirstName, LastName, MiddleName FROM kpcustomersglobal.customers WHERE FirstName = '" + FirstName + "' AND LastName = '" + LastName + "' AND MiddleName = '" + MiddleName + "' AND DATE_FORMAT(BirthDate,'%Y-%m-%d') = '" + Birthdate + "'";

        //throw new Exception(query);
        using (MySqlConnection conn = custconGlobal.getConnection())
        {
            try
            {
                conn.Open();
                MySqlCommand cmd = new MySqlCommand(query, conn);
                MySqlDataReader dataReader = cmd.ExecuteReader();
                while (dataReader.Read())
                {
                    custcounter++;
                }
                dataReader.Close();
                conn.Close();
                kplog.Info("SUCCESS");
                return custcounter;

            }
            catch (MySqlException ex)
            {
                dbconGlobal.CloseConnection();
                //   throw new Exception("getcharge");
                kplog.Fatal("FAILED:: message: " + ex.Message + " ErrorDetail: " + ex.ToString());
                conn.Close();
                return -1;
            }
        }
    }


}