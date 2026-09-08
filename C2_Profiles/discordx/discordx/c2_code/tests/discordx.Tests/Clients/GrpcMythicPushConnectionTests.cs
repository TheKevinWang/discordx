using discordx.Clients;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace discordx.Tests.Clients
{
    [TestClass]
    public class GrpcMythicPushConnectionTests
    {
        [TestMethod]
        public void CreateHttpHandler_DisablesAggressiveTransportKeepalivePings()
        {
            using var handler = GrpcMythicPushConnection.CreateHttpHandler();

            Assert.IsTrue(handler.EnableMultipleHttp2Connections);
            Assert.AreEqual(Timeout.InfiniteTimeSpan, handler.PooledConnectionIdleTimeout);
            Assert.AreEqual(Timeout.InfiniteTimeSpan, handler.KeepAlivePingDelay);
        }
    }
}
