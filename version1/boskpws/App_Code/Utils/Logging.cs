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
using log4net;

/// <summary>
/// Summary description for Logging
/// </summary>
[System.Diagnostics.DebuggerStepThrough]
public static class Logging
{
    /// <summary>Log an Informational Message</summary>
    public static void Info(String message)
    {
        LogManager.GetLogger("root").Info(message);
    }

    /// <summary>Log an Error Message</summary>
    public static void Error(String message, Exception ex)
    {
        //for errors, use the ErrorLog that will write to a flat file (bufferless) rather than database (buffered)
        LogManager.GetLogger("ErrorLogger").Error(message, ex);
    }

    /// <summary>Insert a parameter to MDC</summary>
    public static void SetParameter(String name, String value)
    {
        ThreadContext.Properties[name] = value;
    }

    /// <summary>Remove parameter from MDC</summary>
    public static void RemoveParameter(String name)
    {
        ThreadContext.Properties.Remove(name);
    }

    /// <summary>Add another context to NDC</summary>
    public static void PushContext(Object obj)
    {
        ThreadContext.Stacks["NDC"].Push(obj.ToString());
    }

    /// <summary>Clear all contexts from NDC</summary>
    public static void ClearContext()
    {
        ThreadContext.Stacks["NDC"].Clear();
    }

    /// <summary>Remove top context from NDC</summary>
    public static void PopContext()
    {
        ThreadContext.Stacks["NDC"].Pop();
    }

    /// <summary>Set Execution time parameter (null for zero)</summary>
    public static void SetExecutionTime(DateTime? current)
    {
        Double execution_time_milliseconds = 0;
        if (current != null)
        {
            DateTime start;
            if (DateTime.TryParse(ThreadContext.Properties["start_time"].ToString(), out start))
            {
                Int64 execution_time_ticks = (current.Value - start).Ticks;
                execution_time_milliseconds = (execution_time_ticks * 1.00 / TimeSpan.TicksPerMillisecond);
            }
        }
        ThreadContext.Properties["execution_time"] = execution_time_milliseconds;
    }

    /// <summary>Store context-specific start time for sharing across methods (within a thread)</summary>
    public static void SetStartTime()
    {
        ThreadContext.Properties["start_time"] = DateTime.Now.ToString();
    }
}
