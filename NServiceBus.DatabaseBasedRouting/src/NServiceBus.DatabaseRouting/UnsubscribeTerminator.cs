using System.Threading.Tasks;
using NServiceBus.Logging;
using NServiceBus.Pipeline;

namespace NServiceBus.DatabaseBasedRouting
{
    class UnsubscribeTerminator : PipelineTerminator<IUnsubscribeContext>
    {
        protected override Task Terminate(IUnsubscribeContext context)
        {
            Log.Debug($"Unsubscribe was called for {context.EventType.FullName}. With DatabaseBasedRouting, unsubscribe operations have no effect and subscribers should be configured in the routing file.");
            return Task.CompletedTask;
        }

        static readonly ILog Log = LogManager.GetLogger<DatabaseBasedRoutingFeature>();
    }
}