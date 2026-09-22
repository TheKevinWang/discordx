using Autofac;
using discordx.Clients;
using discordx.Models.Server;

namespace discordx
{
    public static class ContainerBuilder
    {
        public static Autofac.ContainerBuilder Build()
        {
            var containerBuilder = new Autofac.ContainerBuilder();
            containerBuilder.RegisterType<ServerConfig>().As<IServerConfig>().SingleInstance();
            containerBuilder.RegisterType<GrpcMythicPushConnectionFactory>().As<IMythicPushConnectionFactory>().SingleInstance();
            containerBuilder.RegisterType<MythicClient>().As<IMythicClient>().SingleInstance();
            containerBuilder.RegisterType<discordx.Clients.DiscordClient>().As<IDiscordClient>().SingleInstance();
            return containerBuilder;
        }
    }
}
