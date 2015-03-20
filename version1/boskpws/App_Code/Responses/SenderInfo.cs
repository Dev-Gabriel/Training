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
/// Summary description for SenderInfo
/// </summary>
public class SenderInfo
{
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string MiddleName { get; set; }
    public string SenderName { get; set; }
    public string Street { get; set; }
    public string ProvinceCity { get; set; }
    public string Country { get; set; }
    public string Gender { get; set; }
    public string ContactNo { get; set; }
    //public bool IsSMS { get; set; }
    public Int32 IsSMS { get; set; }
    public string Birthdate { get; set; }
    public string BranchID { get; set; }
    public string CustID { get; set; }
    public string SenderMLCardNo { get; set; }
   
}
