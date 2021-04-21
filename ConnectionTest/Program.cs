using System;
using System.Data;
using System.Data.SqlClient;
using NLog;

namespace ConnectionTest
{
	class Program
	{

		private static Logger logger = LogManager.GetCurrentClassLogger();

		static void Main(string[] args)
		{
			logger.Info("Testing Connection");
			if (!TestConnection()) { logger.Info("No Connection Made"); return; };

			Question_1();

			Question_2("feds1");
			Question_2("feds2");
			Question_2("feds3");

			Question_3();
			insertdata(10);
			Question_3();

			logger.Info("Completed Successfully");
		}

		private static void InsertReadings()
        {
			int EquipmentNo = 9;
			logger.Debug($"Insert Readings for Equipment {EquipmentNo}");

			try
            {
				string sql = "Insert Into Readings (ReadingDateTime, Reading, UM, EquipmentNo) VALUES (@dt, @reading, @um, @equipmentno)";
				using SqlConnection conn = GetConnection();
				conn.Open();
				using SqlCommand cmd = new SqlCommand(sql, conn);
				cmd.Parameters.Add(new SqlParameter("@dt", DateTime.Now));
				cmd.Parameters.Add(new SqlParameter("@reading", new Random().NextDouble()));
				cmd.Parameters.Add(new SqlParameter("@um", "A"));
				cmd.Parameters.Add(new SqlParameter("@equipmentno", EquipmentNo));
				int rowsaffected = cmd.ExecuteNonQuery();
				logger.Debug($"Rows Affected: {rowsaffected}");
			}
			catch (Exception e)
            {
				logger.Error(e.StackTrace);
				return;
            }
        }

		private static void Question_1()
		{
			logger.Debug("Question 1:");
			string sql = @"Select * FROM (
	                        SELECT flight_id,
	                        Row_Number() OVER (PARTITION BY flight_id Order By flight_Id)  As RowNo,
	                        Avg(CAST(Altitude as float)) OVER(PARTITION BY flight_id) as AVG_ALT
	                        from feds1 
                        ) AS Tbl
                        WHERE RowNo = 1
                    ";
			using SqlConnection conn = GetConnection();
			conn.Open();
			using SqlCommand cmd = new SqlCommand(sql, conn);
			SqlDataReader sdr = cmd.ExecuteReader();
			try
			{
				if (sdr.HasRows)
				{
					while (sdr.Read())
					{
						Int64 row = Util.DataReaderUtil.GetSafeInt64(sdr, "RowNo");
						double avg = Util.DataReaderUtil.GetSafeDouble(sdr, "AVG_ALT");
						string flightid = Util.DataReaderUtil.GetSafeString(sdr, "flight_id");
						string datarow = $"Flight: {flightid}, Row: {row}, Alt: {avg}";
						logger.Debug(datarow);
					}
				}
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static void Question_2(String feds)
		{
			logger.Debug($"Question 2: {feds}");
			string sql = $"Select flight_id, Avg(Cast(Speed as float)) as AVG_SPEED FROM {feds} Group by flight_id Order by flight_id";
			using SqlConnection conn = GetConnection();
			conn.Open();
			using SqlCommand cmd = new SqlCommand(sql, conn);
			SqlDataReader sdr = cmd.ExecuteReader();
			try
			{
				if (sdr.HasRows)
				{
					while (sdr.Read())
					{
						string flightid = Util.DataReaderUtil.GetSafeString(sdr, "flight_id");
						double speed = Util.DataReaderUtil.GetSafeDouble(sdr, "AVG_SPEED");
						string datarow = $"Flight: {flightid}, Speed: {speed}";
						logger.Debug(datarow);
					}
				}
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static void Question_3()
		{
			logger.Debug($"Question 3:");
			string sql = $"Select Cast(EquipmentNo as float) as EquipNo, Avg(Cast(Reading as float)) as AVG_Reading from Readings Group by EquipmentNo Order by EquipmentNo";
			using SqlConnection conn = GetConnection();
			conn.Open();
			using SqlCommand cmd = new SqlCommand(sql, conn);
			SqlDataReader sdr = cmd.ExecuteReader();
			try
			{
				if (sdr.HasRows)
				{
					while (sdr.Read())
					{
						double equipmentno = Util.DataReaderUtil.GetSafeDouble(sdr, "EquipNo");
						double avg_reading = Util.DataReaderUtil.GetSafeDouble(sdr, "AVG_Reading");
						string datarow = $"Equipment: {equipmentno}, Reading: {avg_reading}";
						logger.Debug(datarow);
					}
				}
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static void insertdata(int equipment)
        {
			using SqlConnection conn = GetConnection();
			conn.Open();
			logger.Debug($"Starting Data Load");
			DataTable table = new DataTable();
			using SqlDataAdapter adapter = new SqlDataAdapter("Select Top 0 * From Readings", conn);
			adapter.Fill(table);

			for (int i = 1; i <= equipment; i++)
            {
				logger.Debug($"Current Equipment Number {i}");
				for (int u = 0; u < 100000; u++)
                {
					DataRow row = table.NewRow();
					row["Guid"] = 0;
					row["ReadingDateTime"] = DateTime.Now;
					row["Reading"] = new Random().NextDouble();
					row["UM"] = "A";
					row["EquipmentNo"] = equipment;
					table.Rows.Add(row);
                }
            }
			logger.Debug($"Copying Data to Server");
			using SqlBulkCopy copy = new SqlBulkCopy(conn)
			{
				DestinationTableName = "Readings"
			};
			copy.WriteToServer(table);
			logger.Debug($"Finalizing Data Load");
		}

		private static bool TestConnection()
		{
			try
			{
				using SqlConnection conn = GetConnection();
				conn.Open();
				logger.Debug("Connected");
			}
			catch(Exception e)
            {
				logger.Error($"Not Connected {e.StackTrace}");
				return false;
			}
			return true;
		}

		private static SqlConnection GetConnection()
		{
			string _connstr = "Server=LAPTOP-EFG50VT7\\SQLEXPRESS;Database=EET4250;Trusted_Connection=True;MultipleActiveResultSets=true;Connection Timeout=60";
			SqlConnection Connection = null;
			try
			{
				Connection = new SqlConnection(_connstr);
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
			}
			return Connection;
		}
	}
}
