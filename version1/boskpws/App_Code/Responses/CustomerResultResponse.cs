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
/// Summary description for CustomerResultResponse
/// </summary>
public class CustomerResultResponse
{
    public int respcode { get; set; }
    public string message { get; set; }
    public string ErrorDetail { get; set; }
    public CustArrayResponse[] Data {get;set;}
    public CustomerSearchResponse CustomerData { get; set; }
    public int nextPage { get; set; }
    public int prevPage { get; set; }
    public String totalPages { get; set; }
}
    