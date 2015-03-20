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
/// Summary description for KYCResponse
/// </summary>
public class KYCResponse
{

    public object respcode;
    public String respmsg;
    public String respdetail;
    public string lastname;
    public string firstname;
    public string middlename;
    public string primary_street;
    public string primary_province;
    public string primary_country;
    public string birthdate;
    public string gender;
    public string mobileno;
    public string id1_idno;
    public string id1_expirydate;
    public string id1_idtype;
    public Object message;

    public KYCResponse()
    {
    }
    public KYCResponse(Int16 rcode, string rmsg, object msg)
    {
        respcode = rcode;
        respmsg = rmsg;
        message = msg;
    }

    public KYCResponse(Int16 rcode, string rmsg, string lname, string fname, string mname, string pstreet, string province, string pcountry, string bdate, string sex, string mobile, string idno, string idexpirydate, string idtype)
    {

        respcode = rcode;
        respmsg = rmsg;
        lastname = lname;
        firstname = fname;
        middlename = mname;
        primary_street = pstreet;
        primary_province = province;
        primary_country = pcountry;
        birthdate = bdate;
        gender = sex;
        mobileno = mobile;
        id1_idno = idno;
        id1_expirydate = idexpirydate;
        id1_idtype = idtype;

    }


}
