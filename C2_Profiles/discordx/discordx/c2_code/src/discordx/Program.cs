using Discord;
using Discord.Commands;
using discordx.Clients;
using Autofac;
using discordx.Models.Server;

namespace C2Send
{
    class Program
    {
        /// <summary>
        /// Main loop
        /// </summary>
        public static void Main(string[] args)
        { 
            //Start the handler
            AsyncMain(args).GetAwaiter().GetResult();
        }
        public static async Task AsyncMain(string[] args)
        {
            var containerBuilder = discordx.ContainerBuilder.Build();
            var container = containerBuilder.Build();
            using (var scope = container.BeginLifetimeScope())
            {
                var discordClient = scope.Resolve<discordx.Models.Server.IDiscordClient>();
                await discordClient.Start();
            }
        }
    }
}
