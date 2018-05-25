using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using NServiceBus.Logging;

namespace NServiceBus.DatabaseBasedRouting
{
    class DatabaseReader
    {
        public DatabaseReader()
        {
        }

        public IEnumerable<EndpointRoutingConfiguration> GetEndpoints(string connectionString)
        {
            var endpoints = ExecuteStoredProcedureAndReturnList(connectionString, "[bus].[SelectEndPoints]", null, EndpointMapper);

            foreach (var endpoint in endpoints)
            {
                var sqlParameters1 = new List<SqlParameter>()
                {
                    new SqlParameter("@EndpointId", SqlDbType.Int) {Value = endpoint.EndpointId},
                };
                //Get Events
                var events = ExecuteStoredProcedureAndReturnList(connectionString, "[bus].[SelectEventsByEndpointId]", sqlParameters1.ToArray(), EventMapper);
                endpoint.Events = events;

                var sqlParameters2 = new List<SqlParameter>()
                {
                    new SqlParameter("@EndpointId", SqlDbType.Int) {Value = endpoint.EndpointId},
                };
                //Get Commands
                var commands = ExecuteStoredProcedureAndReturnList(connectionString, "[bus].[SelectCommandsByEndpointId]", sqlParameters2.ToArray(), CommandMapper);
                endpoint.Commands = commands;
            }

            //return endpoints;
            var configs = new List<EndpointRoutingConfiguration>();

            foreach (var endpoint in endpoints)
            {
                var config = new EndpointRoutingConfiguration
                {
                    LogicalEndpointName = endpoint.EndpointName
                };

                config.Commands = endpoint.Commands.Select(c => Type.GetType(c.CommandName, true)).ToArray();
                config.Events = endpoint.Events.Select(c => Type.GetType(c.EventName, true)).ToArray();
                configs.Add(config);
            }

            return configs;
        }

        public static List<T> ExecuteStoredProcedureAndReturnList<T>(string connectionString, string storeProcedure,
            SqlParameter[] parameterDictionary, Func<IDataRecord, T> map)
        {
            List<T> list = new List<T>();
            using (SqlConnection sqlConnection = new SqlConnection(connectionString))
            {
                using (SqlCommand sqlCommand = new SqlCommand(storeProcedure, sqlConnection))
                {
                    sqlConnection.Open();
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    if (parameterDictionary != null) sqlCommand.Parameters.AddRange(parameterDictionary);

                    using (var reader = sqlCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var mapping = map(reader);
                            list.Add(mapping);
                        }
                    }
                }
            }
            return list;
        }

        #region Mappers
        private ServiceBusEndpoint EndpointMapper(IDataRecord reader)
        {
            var endpoint = new ServiceBusEndpoint
            {
                EndpointId = reader.GetInt32(reader.GetOrdinal("EndpointId")),
                EndpointShortname = reader.GetString(reader.GetOrdinal("EndpointShortname")),
                EndpointName = reader.GetString(reader.GetOrdinal("EndpointName")),
                DllFolder = reader.GetString(reader.GetOrdinal("DllFolder")),
            };
            return endpoint;
        }

        private ServiceBusEvent EventMapper(IDataRecord reader)
        {
            var data = new ServiceBusEvent
            {
                EventName = reader.GetString(reader.GetOrdinal("EventName")),
            };
            return data;
        }

        private ServiceBusCommand CommandMapper(IDataRecord reader)
        {
            var data = new ServiceBusCommand
            {
                CommandName = reader.GetString(reader.GetOrdinal("CommandName")),
            };
            return data;
        }
        #endregion

        static readonly ILog Logger = LogManager.GetLogger(typeof(DatabaseReader));
    }

    public class ServiceBusEndpoint
    {
        public int EndpointId { get; set; }
        public string EndpointShortname { get; set; }

        public string EndpointName { get; set; }

        public string DllFolder { get; set; }
        public List<ServiceBusCommand> Commands { get; set; }
        public List<ServiceBusEvent> Events { get; set; }
    }

    public class ServiceBusEvent
    {
        public string EventName { get; set; }
    }

    public class ServiceBusCommand
    {
        public string CommandName { get; set; }
    }
}