package main

import (
	discordxfunctions "DiscordxContainer/discordx/c2functions"

	"github.com/MythicMeta/MythicContainer"
)

func main() {
	discordxfunctions.Initialize()
	MythicContainer.StartAndRunForever([]MythicContainer.MythicServices{
		MythicContainer.MythicServiceC2,
	})
}
