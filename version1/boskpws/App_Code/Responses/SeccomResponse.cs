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
/// Summary description for SeccomResponse
/// </summary>
public class SeccomResponse
{
    public int respcode { get; set; }
    public String message { get; set; }
    public String ErrorDetail { get; set; }

}
