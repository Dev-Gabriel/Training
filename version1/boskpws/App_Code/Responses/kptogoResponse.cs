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
/// Summary description for kptogoResponse
/// </summary>
public class kptogoResponse
{
    public int respcode { get; set; }
    public String message { get; set; }
    public String ErrorDetail { get; set; }
    public Decimal amount { get; set; }
    public String date { get; set; }
    public String currency { get; set; }
    public String status { get; set; }
    public String pin { get; set; }
    public String serial { get; set; }
    public String requestno { get; set; }
}
