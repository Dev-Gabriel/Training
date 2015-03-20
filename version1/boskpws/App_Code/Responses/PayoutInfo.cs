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
/// Summary description for PayoutInfo
/// </summary>
public class PayoutInfo
{

    public string SendoutControlNo { get; set; }
    public string KPTNNo { get; set; }
    public string OperatorID { get; set; }
    public bool IsPassword { get; set; }
    public string TransPassword { get; set; }
    //public DateTime syscreated { get; set; }
    public string Currency { get; set; }
    public Decimal Principal { get; set; }
    public string SenderID { get; set; }
    public Int32 SenderIsSMS { get; set; }
    //public string ReceiverID { get; set; }
    public bool ReceiverIsSMS { get; set; }
    public string Relation { get; set; }
    public string Message { get; set; }
    public string StationID { get; set; }
    public string SourceOfFund { get; set; }
    public string kptn4 { get; set; }
    public string IDType { get; set; }
    public string IDNo { get; set; }
    public string ExpiryDate { get; set; }
    public Decimal DormantCharge { get; set; }
    public string ORNo { get; set; }
    public string Purpose { get; set; }
    public Decimal Charge { get; set; }
    public Decimal OtherCharge { get; set; }
    public Decimal Total { get; set; }
    public string RemoteBranch { get; set; }
    public string RemoteOperatorID { get; set; }
    public string ControlNo { get; set; }
    public string BranchCode { get; set; }
    public string ClaimedDate { get; set; }
    public string SODate { get; set; }
    public string SOORNo { get; set; }
    public string SOMessage { get; set; }
    public Int32 ZoneCode { get; set; }
    public bool IsRemote { get; set; }
    public Decimal Balance { get; set; }
    public Int32? RemoteZone { get; set; }
    public String prefPO { get; set; }
    public Decimal amtPO { get; set; }
    public Decimal exchangerate { get; set; }
}
