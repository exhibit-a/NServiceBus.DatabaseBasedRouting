using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Features;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NServiceBus.Logging;

namespace NServiceBus.DatabaseBasedRouting
{
    class DatabaseBasedRoutingFeature : Feature
    {
        static ILog _log = LogManager.GetLogger<DatabaseBasedRoutingFeature>();

        public const string DatabaseConnectionKey = "NServiceBus.DatabaseBasedRouting.ConnectionString";
        public const string RouteFileUpdateInterval = "NServiceBus.DatabaseBasedRouting.RouteFileUpdateInterval";
        internal const string MessageDrivenSubscriptionsEnabled = "NServiceBus.DatabaseBasedRouting.MessageDrivenSubscriptionsEnabled";

        public DatabaseBasedRoutingFeature()
        {
            Defaults(s =>
            {
                s.SetDefault(DatabaseConnectionKey, UriHelper.FilePathToUri("endpoints.xml"));
                s.SetDefault(RouteFileUpdateInterval, TimeSpan.FromSeconds(30));
                s.SetDefault(MessageDrivenSubscriptionsEnabled, false);
                s.SetDefault<UnicastSubscriberTable>(new UnicastSubscriberTable());
            });
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            var transportInfrastructure = context.Settings.Get<TransportInfrastructure>();

            var unicastRoutingTable = context.Settings.Get<UnicastRoutingTable>();
            var unicastSubscriberTable = context.Settings.Get<UnicastSubscriberTable>();

            var routeFileUpdateInterval = context.Settings.Get<TimeSpan>(RouteFileUpdateInterval);
            var connectionString = context.Settings.Get<string>(DatabaseConnectionKey);
            var databaseReader = new DatabaseReader();

            var nativeSends = transportInfrastructure.OutboundRoutingPolicy.Sends == OutboundRoutingType.Multicast;
            var nativePublishes = transportInfrastructure.OutboundRoutingPolicy.Publishes == OutboundRoutingType.Multicast;

            // ensure the routing file is valid and the routing table is populated before running FeatureStartupTasks
            UpdateRoutingTable(databaseReader, connectionString, unicastRoutingTable, unicastSubscriberTable, nativeSends, nativePublishes);

            if (routeFileUpdateInterval > TimeSpan.Zero)
            {
                context.RegisterStartupTask(new UpdateRoutingTask(() => UpdateRoutingTable(databaseReader, connectionString, unicastRoutingTable, unicastSubscriberTable, nativeSends, nativePublishes), routeFileUpdateInterval));
            }

            // if the transport provides native pub/sub support, don't plug in the Database pub/sub storage.
            if (context.Settings.Get<TransportInfrastructure>().OutboundRoutingPolicy.Publishes == OutboundRoutingType.Unicast)
            {
                var routingConnector = new PublishRoutingConnector(
                    unicastSubscriberTable,
                    context.Settings.Get<EndpointInstances>(),
                    context.Settings.Get<DistributionPolicy>(),
                    instance => transportInfrastructure.ToTransportAddress(LogicalAddress.CreateRemoteAddress(instance)));

                context.Pipeline.Replace("UnicastPublishRouterConnector", routingConnector);
                if (!context.Settings.Get<bool>(MessageDrivenSubscriptionsEnabled))
                {
                    context.Pipeline.Replace("MessageDrivenSubscribeTerminator", new SubscribeTerminator(), "handles subscribe operations");
                    context.Pipeline.Replace("MessageDrivenUnsubscribeTerminator", new UnsubscribeTerminator(), "handles ubsubscribe operations");
                }

            }
        }

        static void UpdateRoutingTable(DatabaseReader databaseReader, string connectionString, UnicastRoutingTable routingTable, UnicastSubscriberTable subscriberTable, bool nativeSends, bool nativePublishes)
        {
            try
            {
                var endpoints = databaseReader.GetEndpoints(connectionString);

                var commandRoutes = new List<RouteTableEntry>();
                var eventRoutes = new List<RouteTableEntry>();

                foreach (var endpoint in endpoints)
                {
                    var route = UnicastRoute.CreateFromEndpointName(endpoint.LogicalEndpointName);
                    foreach (var commandType in endpoint.Commands)
                    {
                        if (nativeSends)
                        {
                            _log.Warn($"Selected transport uses native command routing. Route for {commandType.FullName} to {endpoint.LogicalEndpointName} configured in database will be ignored.");
                        }
                        commandRoutes.Add(new RouteTableEntry(commandType, route));
                    }

                    foreach (var eventType in endpoint.Events)
                    {
                        if (nativePublishes)
                        {
                            _log.Warn($"Selected transport uses native event routing. Route for {eventType.FullName} to {endpoint.LogicalEndpointName} configured in database will be ignored.");
                        }
                        eventRoutes.Add(new RouteTableEntry(eventType, route));
                    }
                }

                routingTable.AddOrReplaceRoutes("DatabaseBasedRouting", commandRoutes);
                subscriberTable.AddOrReplaceRoutes("DatabaseBasedRouting", eventRoutes);

                _log.Debug($"Updated routing information from database");
            }
            catch (Exception e)
            {
                _log.Error($"Failed to update routing information from database. The last valid routing configuration will be used instead.", e);
                throw;
            }
        }

        class UpdateRoutingTask : FeatureStartupTask, IDisposable
        {
            Action _updateRoutingCallback;
	        TimeSpan _routeFileUpdateInterval;
	        Timer _updateTimer;

            public UpdateRoutingTask(Action updateRoutingCallback, TimeSpan routeFileUpdateInterval)
            {
	            this._updateRoutingCallback = updateRoutingCallback;
	            this._routeFileUpdateInterval = routeFileUpdateInterval;
            }

            protected override Task OnStart(IMessageSession session)
            {
                _updateTimer = new Timer(state =>
                {
                    try
                    {
                        _updateRoutingCallback();
                    }
                    catch (Exception)
                    {
                        // ignore exceptions to prevent endpoint crashes
                    }
                }, null, _routeFileUpdateInterval, _routeFileUpdateInterval);

                return Task.CompletedTask;
            }

            protected override Task OnStop(IMessageSession session)
            {
                _updateTimer?.Dispose();
                _updateTimer = null;

                return Task.CompletedTask;
            }

            public void Dispose()
            {
                _updateTimer?.Dispose();
                _updateTimer = null;
            }
        }
    }
}