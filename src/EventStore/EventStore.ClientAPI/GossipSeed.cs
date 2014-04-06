using System.Net;

namespace EventStore.ClientAPI
{
	/// <summary>
	/// Represents the address of an endpoint and any necessary host header
	/// needed to make HTTP requests to it.
	/// </summary>
	public class GossipSeed
	{
		/// <summary>
		/// The <see cref="IPEndPoint" /> on which to connect.
		/// </summary>
		public readonly IPEndPoint Endpoint;

		/// <summary>
		/// The host header necessary for making requests to the endpoint. If this is empty,
		/// no host header will be set when requesting gossip.
		/// </summary>
		public readonly string HostHeader;

		public GossipSeed(IPAddress address, int port, string hostHeader = "")
		{
			Endpoint = new IPEndPoint(address, port);
			HostHeader = hostHeader;
		}
	}
}
