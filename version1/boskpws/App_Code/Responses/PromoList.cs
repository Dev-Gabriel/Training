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
/// Summary description for PromoList
/// </summary>
public class PromoList
{
    public string promoName { get; set; }
    public string effectiveDate { get; set; }
    public string expiryDate { get; set; }
    public Boolean active { get; set; }
}
