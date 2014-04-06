using System;
using System.Net;

namespace EventStore.ClientAPI
{
    /// <summary>
    /// Fluent builder used to configure <see cref="ClusterSettings" /> for connecting to a cluster
    /// using gossip seeds.
    /// </summary>
    public class GossipSeedClusterSettingsBuilder
    {
        private GossipSeed[] _gossipSeeds;
        private TimeSpan _gossipTimeout = TimeSpan.FromSeconds(1);
        private int _maxDiscoverAttempts = Consts.DefaultMaxClusterDiscoverAttempts;

        /// <summary>
        /// Sets gossip seed endpoints for the client. If you need to use a Host header to
		/// be able to make HTTP requests to the Event Store, use the overload of this method
		/// which takes <see cref="GossipSeed" />[] instead.
        /// </summary>
        /// <param name="gossipSeeds"><see cref="IPEndPoint" />s representing the endpoints of nodes from which to seed gossip.</param>
        /// <returns>A <see cref="GossipSeedClusterSettingsBuilder"/> for further configuration.</returns>
        /// <exception cref="ArgumentException">If no gossip seeds are specified.</exception>
        public GossipSeedClusterSettingsBuilder SetGossipSeedEndPoints(params IPEndPoint[] gossipSeeds)
        {
            if (gossipSeeds == null || gossipSeeds.Length == 0)
                throw new ArgumentException("The array of gossip seeds cannot be null or empty.");

			var seeds = new GossipSeed[gossipSeeds.Length];
            for (var i = 0; i < gossipSeeds.Length; i++)
			{
				var seed = gossipSeeds[i];
				seeds[i] = new GossipSeed(seed.Address, seed.Port);
			}
			_gossipSeeds = seeds;
			return this;
        }

        /// <summary>
        /// Sets gossip seed endpoints for the client. 
        /// </summary>
        /// <param name="gossipSeeds"><see cref="GossipSeed" />s representing the endpoints of nodes from which to seed gossip.</param>
        /// <returns>A <see cref="GossipSeedClusterSettingsBuilder"/> for further configuration.</returns>
        /// <exception cref="ArgumentException">If no gossip seeds are specified.</exception>
		public GossipSeedClusterSettingsBuilder SetGossipSeedEndPoints(params GossipSeed[] gossipSeeds)
		{
			if (gossipSeeds == null || gossipSeeds.Length == 0)
				throw new ArgumentException("The array of gossip seeds cannot be null or empty.");

			_gossipSeeds = gossipSeeds;
			return this;
		}

        /// <summary>
        /// Sets the maximum number of attempts for discovery.
        /// </summary>
        /// <param name="maxDiscoverAttempts">The maximum number of attempts for DNS discovery.</param>
        /// <returns>A <see cref="GossipSeedClusterSettingsBuilder"/> for further configuration.</returns>
        /// <exception cref="ArgumentOutOfRangeException">If <see cref="maxDiscoverAttempts" /> is less than or equal to 0.</exception>
        public GossipSeedClusterSettingsBuilder SetMaxDiscoverAttempts(int maxDiscoverAttempts)
        {
            if (maxDiscoverAttempts <= 0)
                throw new ArgumentOutOfRangeException("maxDiscoverAttempts", string.Format("maxDiscoverAttempts value is out of range: {0}. Allowed range: [-1, infinity].", maxDiscoverAttempts));
            _maxDiscoverAttempts = maxDiscoverAttempts;
            return this;
        }

        /// <summary>
        /// Sets the period after which gossip times out if none is received.
        /// </summary>
        /// <param name="timeout">The period after which gossip times out if none is received.</param>
        /// <returns>A <see cref="GossipSeedClusterSettingsBuilder"/> for further configuration.</returns>
        public GossipSeedClusterSettingsBuilder SetGossipTimeout(TimeSpan timeout)
        {
            _gossipTimeout = timeout;
            return this;
        }

        /// <summary>
        /// Builds a <see cref="ClusterSettings"/> object from a <see cref="GossipSeedClusterSettingsBuilder"/>.
        /// </summary>
        /// <param name="builder"><see cref="GossipSeedClusterSettingsBuilder"/> from which to build a <see cref="ClusterSettings"/></param>
        /// <returns></returns>
        public static implicit operator ClusterSettings(GossipSeedClusterSettingsBuilder builder)
        {
            return new ClusterSettings(builder._gossipSeeds, builder._maxDiscoverAttempts, builder._gossipTimeout);
        }
    }
}
