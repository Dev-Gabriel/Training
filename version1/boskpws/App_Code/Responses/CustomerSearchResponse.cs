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
/// Summary description for CustomerSearchResponse
/// </summary>
public class CustomerSearchResponse
{
    public string MLCardNo { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string MiddleName { get; set; }
    public string Street { get; set; }
    public string ProvinceCity { get; set; }
    public string BirthDate { get; set; }
    public string Gender { get; set; }
    public string ContactNo { get; set; }
    public string Country { get; set; }
    public string Zipcode { get; set; }
    public string IDType { get; set; }
    public string IDNo { get; set; }
    public string ExpiryDate { get; set; }
    public string CustID { get; set; }
    public string PhoneNo { get; set; }
    public string Mobile { get; set; }
    public string Email { get; set; }
    public string dtissued { get; set; }
    public string placeissued { get; set; }
    public string secondidtype { get; set; }
    public string secondidno { get; set; }
    public string secondplaceissued { get; set; }
    public string secondissueddate { get; set; }
    public string secondexpirydate { get; set; }
    public string homecity { get; set; }
    public string workstreet { get; set; }
    public string workprovcity { get; set; }
    public string workcity { get; set; }
    public string workcountry { get; set; }
    public string workzipcode { get; set; }
    public string occupation { get; set; }
    public string ssn { get; set; }
    public string sourceofincome { get; set; }
    public string relation { get; set; }
    public string proofoffunds { get; set; }

    public int respcode { get; set; }
    public string message { get; set; }
}
