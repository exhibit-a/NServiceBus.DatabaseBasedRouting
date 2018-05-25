using System;

namespace NServiceBus.DatabaseBasedRouting
{
    class EndpointRoutingConfiguration
    {
        public string LogicalEndpointName { get; set; }

        public Type[] Commands { get; set; }

        public Type[] Events { get; set; }
    }
}