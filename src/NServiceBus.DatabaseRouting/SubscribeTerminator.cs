using System.Threading.Tasks;
using NServiceBus.Features;
using NServiceBus.Logging;
using NServiceBus.Pipeline;

namespace NServiceBus.DatabaseBasedRouting
{
    class SubscribeTerminator : PipelineTerminator<ISubscribeContext>
    {
        protected override Task Terminate(ISubscribeContext context)
        {
            Log.Debug($"Subscribe was called for {context.EventType.FullName}. With DatabaseBasedRouting, subscribe operations have no effect and subscribers should be configured in the routing file. If subscribe was not called by you, consider disabling the {nameof(AutoSubscribe)} feature.");
            return Task.CompletedTask;
        }

        static readonly ILog Log = LogManager.GetLogger<DatabaseBasedRoutingFeature>();
    }
}