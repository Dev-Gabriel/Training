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
/// Summary description for ReceiverInfo
/// </summary>
public class ReceiverInfo
{
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string MiddleName { get; set; }
    public string ReceiverName { get; set; }
    public string Street { get; set; }
    public string ProvinceCity { get; set; }
    public string Country { get; set; }
    public string Gender { get; set; }
    public string ContactNo { get; set; }
    public string BirthDate { get; set; }
    public string MLCardNo { get; set; }
}
