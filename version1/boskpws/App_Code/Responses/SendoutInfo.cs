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
/// Summary description for SendoutInfo
/// </summary>
public class SendoutInfo
{
    public string SendoutControlNo { get; set; }
    public string KPTNNo { get; set; }
    public string OperatorID { get; set; }
    public bool IsPassword { get; set; }
    public string TransPassword { get; set; }
    public DateTime syscreated { get; set; }
    public string Currency { get; set; }
    public Decimal Principal { get; set; }
    public string SenderID { get; set; }
    public Int32 SenderIsSMS { get; set; }
    //public string ReceiverID { get; set; }
    public Int32 ReceiverIsSMS { get; set; }
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
    public bool IsRemote { get; set; }
    public string RemoteBranch { get; set; }
    public string RemoteOperatorID { get; set; }
    public string ControlNo { get; set; }
    public string BranchCode { get; set; }
    public Decimal Redeem { get; set; }
    public Boolean isSameAmount { get; set; }
    public Int32 ZoneCode { get; set; }
    public Decimal CancelCharge { get; set; }
    public String RemoteReason { get; set; }
    public Int32? RemoteZone { get; set; }
    public double? vat { get; set; }
    public String preferredcurrency { get; set; }
    public double? exchangerate { get; set; }
    public double? amountpo { get; set; }
    public String paytype { get; set; }
    public String bankname { get; set; }
    public String cardcheck { get; set; }
    public String cardexp { get; set; }
    public Decimal xchangerate { get; set; }

    public String senderbankaccount { get; set; }
    public String senderbankcode { get; set; }
    public String transtp { get; set; }
}
