using NServiceBus.Configuration.AdvancedExtensibility;

namespace NServiceBus.DatabaseBasedRouting
{
    using Features;

    public static class DatabaseBasedRoutingConfigExtensions
    {
        /// <summary>
        /// Enables routing configured with the routing configuration file.
        /// </summary>
        /// <param name="config">The configuration object.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config)
        {
            var settings = config.GetSettings();
            settings.EnableFeatureByDefault<DatabaseBasedRoutingFeature>();
            return new DatabaseBasedRoutingOptions(settings);
        }

        /// <summary>
        /// Enables routing configured with the routing configuration file under <paramref name="configurationFilePath"/>
        /// </summary>
        /// <param name="config">The configuration object.</param>
        /// <param name="connectionString">The connection string to the routing configuration database.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config, string connectionString)
        {
            var settings = config.GetSettings();
            settings.Set(DatabaseBasedRoutingFeature.DatabaseConnectionKey, connectionString);
            settings.EnableFeatureByDefault<DatabaseBasedRoutingFeature>();
            return new DatabaseBasedRoutingOptions(settings);
        }
        
        /*
        /// <summary>
        /// Enables routing configured with the routing configuration file.
        /// </summary>
        /// <param name="config">The configuration object.</param>
        /// <param name="updateInterval">The interval the route file should be checked for changes.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config, TimeSpan updateInterval)
        {
            if (updateInterval < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(updateInterval), "Update interval cannot be negative.");

            var settings = config.GetSettings();
            settings.Set(DatabaseBasedRoutingFeature.RouteFileUpdateInterval, updateInterval);
            settings.EnableFeatureByDefault<DatabaseBasedRoutingFeature>();
            return new DatabaseBasedRoutingOptions(settings);
        }

        /// <summary>
        /// Enables routing configured with the routing configuration file under <paramref name="configurationFilePath"/>
        /// </summary>
        /// <param name="config">The configuration object.</param>
        /// <param name="configurationFilePath">The path to the configuration file.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config, string configurationFilePath)
        {
            return config.UseDatabaseBasedRouting(UriHelper.FilePathToUri(configurationFilePath));
        }

        /// <summary>
        /// Enables routing configured with the routing configuration file under <paramref name="configurationFilePath"/>
        /// </summary>
        /// <param name="config">The configuration object.</param>
        /// <param name="configurationFilePath">The path to the configuration file.</param>
        /// <param name="updateInterval">The interval the route file should be checked for changes.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config, string configurationFilePath, TimeSpan updateInterval)
        {
            return config.UseDatabaseBasedRouting(UriHelper.FilePathToUri(configurationFilePath), updateInterval);
        }

        /// <summary>
        /// Enables routing configured with the routing configuration file under <paramref name="configurationFileUri"/>
        /// </summary>
        /// <param name="config">The configuration object.</param>
        /// <param name="configurationFileUri">The <see cref="Uri"/> to the configuration file.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config, Uri configurationFileUri)
        {
            var settings = config.GetSettings();
            settings.Set(DatabaseBasedRoutingFeature.DatabaseConnectionKey, configurationFileUri);
            settings.EnableFeatureByDefault<DatabaseBasedRoutingFeature>();
            return new DatabaseBasedRoutingOptions(settings);
        }

        /// <summary>
        /// Enables routing configured with the routing configuration file under <paramref name="configurationFileUri"/>
        /// </summary>
        /// <param name="config">The configuration object.</param>
        /// <param name="configurationFileUri">The <see cref="Uri"/> to the configuration file.</param>
        /// <param name="updateInterval">The interval the route file should be checked for changes.</param>
        public static DatabaseBasedRoutingOptions UseDatabaseBasedRouting(this RoutingSettings config, Uri configurationFileUri, TimeSpan updateInterval)
        {
            if (updateInterval < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(updateInterval), "Update interval cannot be negative.");

            var settings = config.GetSettings();
            settings.Set(DatabaseBasedRoutingFeature.RouteFileUpdateInterval, updateInterval);
            settings.Set(DatabaseBasedRoutingFeature.DatabaseConnectionKey, configurationFileUri);
            settings.EnableFeatureByDefault<DatabaseBasedRoutingFeature>();
            return new DatabaseBasedRoutingOptions(settings);
        }
        */
    }
}