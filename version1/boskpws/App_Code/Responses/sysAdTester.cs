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

/// <summary>
/// Summary description for sysAdTester
/// </summary>
public class sysAdTester
{
    public int respcode { get; set; }
    public String message { get; set; }
    public String errorDetail { get; set; }
    public int numberOfDBs { get; set; }
    public String DBConnection { get; set; }
    public String ConnectionStringTransaction { get; set; }
    public String ConnectionStringCustomer { get; set; }
    public String Version { get; set; }
}
