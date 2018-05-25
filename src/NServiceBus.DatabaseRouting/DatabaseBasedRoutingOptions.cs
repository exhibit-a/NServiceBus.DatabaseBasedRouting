namespace NServiceBus.DatabaseBasedRouting
{
    using Settings;

    /// <summary>
    /// Provides configuration options for DatabaseBasedRouting feature
    /// </summary>
    public class DatabaseBasedRoutingOptions
    {
        SettingsHolder _settings;
        public DatabaseBasedRoutingOptions(SettingsHolder settings)
        {
            this._settings = settings;
        }

        /// <summary>
        /// Enables Subscriptions from code even when using file based routing.
        /// </summary>
        public void EnableMessageDrivenSubscriptions()
        {
            _settings.Set(DatabaseBasedRoutingFeature.MessageDrivenSubscriptionsEnabled, true);
        }
    }
}