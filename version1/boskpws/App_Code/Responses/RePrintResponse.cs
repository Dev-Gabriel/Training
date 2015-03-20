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
/// Summary description for RePrintResponse
/// </summary>
public class RePrintResponse
{
    public int respcode { get; set; }
    public String message { get; set; }
    public String ErrorDetail { get; set; }
    public SenderInfo SenderInfo { get; set; }
    public ReceiverInfo ReceiverInfo { get; set; }
    public SendoutInfo SendoutInfo { get; set; }
}
