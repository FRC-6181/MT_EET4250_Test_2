using System;
using System.Data;

namespace ConnectionTest.Util
{
    internal class DataReaderUtil
    {
        /// <summary>
        /// Returns a String or Empty String if Column is Null
        /// </summary>
        /// <param name="sdr">DateReader</param>
        /// <param name="colName">Column Name</param>
        /// <returns></returns>
        public static string GetSafeString(IDataReader sdr, string colName)
        {
            string result = string.Empty;
            if (!sdr.IsDBNull(sdr.GetOrdinal(colName)))
            {
                result = sdr.GetString(sdr.GetOrdinal(colName));
            }
            return result.Trim();
        }


        /// <summary>
        /// Returns an Int32 or Int32.MinValue if Column is Null
        /// </summary>
        /// <param name="sdr">DateReader</param>
        /// <param name="colName">Column Name</param>
        /// <returns></returns>
        public static int GetSafeInt32(IDataReader sdr, string colName)
        {
            int result = int.MinValue;
            if (!sdr.IsDBNull(sdr.GetOrdinal(colName)))
            {
                result = sdr.GetInt32(sdr.GetOrdinal(colName));
            }
            return result;
        }

        /// <summary>
        /// Returns a long or Int64.MinValue if Column is Null
        /// </summary>
        /// <param name="sdr">DateReader</param>
        /// <param name="colName">Column Name</param>
        /// <returns></returns>
        public static long GetSafeInt64(IDataReader sdr, string colName)
        {
            long result = long.MinValue;
            if (!sdr.IsDBNull(sdr.GetOrdinal(colName)))
            {
                result = sdr.GetInt64(sdr.GetOrdinal(colName));
            }
            return result;
        }

        /// <summary>
        /// Returns an Double or Double.MinValue if Column is Null
        /// </summary>
        /// <param name="sdr">DateReader</param>
        /// <param name="colName">Column Name</param>
        /// <returns></returns>
        public static double GetSafeDouble(IDataReader sdr, string colName)
        {
            double result = double.MinValue;
            if (!sdr.IsDBNull(sdr.GetOrdinal(colName)))
            {
                result = sdr.GetDouble(sdr.GetOrdinal(colName));
            }
            return result;
        }

        /// <summary>
        /// Returns an Decimal or Decimal.Zero if Column is Null
        /// </summary>
        /// <param name="sdr">DateReader</param>
        /// <param name="colName">Column Name</param>
        /// <returns></returns>
        public static decimal GetSafeDecimal(IDataReader sdr, string colName)
        {
            decimal result = decimal.Zero;
            if (!sdr.IsDBNull(sdr.GetOrdinal(colName)))
            {
                result = sdr.GetDecimal(sdr.GetOrdinal(colName));
            }
            return result;
        }

        /// <summary>
        /// Returns an Decimal or Decimal.Zero if Column is Null
        /// </summary>
        /// <param name="sdr">DateReader</param>
        /// <param name="colName">Column Name</param>
        /// <returns></returns>
        public static DateTime? GetSafeDateTime(IDataReader sdr, string colName)
        {
            DateTime? result = null;
            if (!sdr.IsDBNull(sdr.GetOrdinal(colName)))
            {
                result = sdr.GetDateTime(sdr.GetOrdinal(colName));
            }
            return result;
        }
    }
}
