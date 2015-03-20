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
using System.Collections.Generic;

/// <summary>
/// Summary description for ChargeResponse
/// </summary>
public class ChargeResponse
{
    public int respcode { get; set; }
    public String message { get; set; }
    public Decimal charge { get; set; }
    public String ErrorDetail { get; set; }
    public PromoList[] promos { get; set; }
    public Double forexrate { get; set; }
}
