using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NServiceBus.Routing;

namespace NServiceBus.DatabaseBasedRouting
{
    class UnicastSubscriberTable
    {
        static readonly UnicastRouteGroup[] EmptyResult =
        {
        };

        public UnicastRouteGroup[] GetRoutesFor(Type messageType)
        {
            UnicastRouteGroup[] unicastRoutes;
            return _routeTable.TryGetValue(messageType, out unicastRoutes)
                ? unicastRoutes
                : EmptyResult;
        }

        public void AddOrReplaceRoutes(string sourceKey, IList<RouteTableEntry> entries)
        {
            // The algorithm uses ReaderWriterLockSlim. First entries are read. If then exists they are compared with passed entries and skipped if equal.
            // Otherwise, the write path is used. It's possible than one thread will execute all the work
            var existing = GetExistingRoutes(sourceKey);
            if (existing != null && existing.SequenceEqual(entries))
            {
                return;
            }

            _readerWriterLock.EnterWriteLock();
            try
            {
                _routeGroups[sourceKey] = entries;
                _routeTable = CalculateNewRouteTable();
            }
            finally
            {
                _readerWriterLock.ExitWriteLock();
            }
        }

        IList<RouteTableEntry> GetExistingRoutes(string sourceKey)
        {
            IList<RouteTableEntry> existing;
            _readerWriterLock.EnterReadLock();
            try
            {
                _routeGroups.TryGetValue(sourceKey, out existing);
            }
            finally
            {
                _readerWriterLock.ExitReadLock();
            }
            return existing;
        }

        Dictionary<Type, UnicastRouteGroup[]> CalculateNewRouteTable()
        {
            var newRouteTable = new Dictionary<Type, List<UnicastRoute>>();
            foreach (var entry in _routeGroups.Values.SelectMany(g => g))
            {
                List<UnicastRoute> typeRoutes;
                if (!newRouteTable.TryGetValue(entry.MessageType, out typeRoutes))
                {
                    typeRoutes = new List<UnicastRoute>();
                    newRouteTable[entry.MessageType] = typeRoutes;
                }
                typeRoutes.Add(entry.Route);
            }
            return newRouteTable.ToDictionary(kvp => kvp.Key, kvp => GroupByEndpoint(kvp.Value));
        }

        static UnicastRouteGroup[] GroupByEndpoint(List<UnicastRoute> routes)
        {
            return routes.GroupBy(r => r.Endpoint)
                .Select(g => new UnicastRouteGroup(g.Key, g.ToArray()))
                .ToArray();
        }

        volatile Dictionary<Type, UnicastRouteGroup[]> _routeTable = new Dictionary<Type, UnicastRouteGroup[]>();
        Dictionary<string, IList<RouteTableEntry>> _routeGroups = new Dictionary<string, IList<RouteTableEntry>>();
        ReaderWriterLockSlim _readerWriterLock = new ReaderWriterLockSlim();
    }
}