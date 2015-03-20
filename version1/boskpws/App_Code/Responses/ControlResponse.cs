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
/// Summary description for ControlResponse
/// </summary>
public class ControlResponse
{
    public int respcode { get; set; }
    public String message { get; set; }
    public String ErrorDetail { get; set; }
    public String controlno { get; set; }
    public String nseries { get; set; }


}
