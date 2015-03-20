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
using System.Diagnostics;

/// <summary>
/// Summary description for HiResDateTime
/// </summary>
public class HiResDateTime
{
    private static DateTime _startTime;
    private static Stopwatch _stopWatch = null;
    private static TimeSpan _maxIdle =
        TimeSpan.FromSeconds(10);

    public static DateTime UtcNow
    {
        get
        {
            if ((_stopWatch == null) ||
                (_startTime.Add(_maxIdle) < DateTime.UtcNow))
            {
                Reset();
            }
            return _startTime.AddTicks(_stopWatch.Elapsed.Ticks);
        }
    }

    private static void Reset()
    {
        _startTime = DateTime.UtcNow;
        _stopWatch = Stopwatch.StartNew();
    }
}
