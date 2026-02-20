import modulefinder
import os
import discord
from discord.ext import commands
from slash_commands import VCSlashCommands, load_config
import dotenv
dotenv.load_dotenv()




class VCControl:
    def __init__(self, token: str | None = None):
        self.token = token
        self.intents = discord.Intents.all()
        self.intents.message_content = True
        self.bot = commands.Bot(command_prefix=commands.when_mentioned, intents=self.intents)
        # register on_ready and add the slash command Cog
        self.bot.add_listener(self.on_ready, name="on_ready")
        

    async def on_ready(self):
        # Add the cog if it hasn't been added yet
        if not any(isinstance(c, VCSlashCommands) for c in self.bot.cogs.values()):
            from slash_commands import VCSlashCommands
            await self.bot.add_cog(VCSlashCommands(self.bot))  # ✅ Await it

        # Sync slash commands for the guild
        try:
            await self.bot.tree.sync()
            print(f"Synced command tree for {self.bot.user} (ID: {self.bot.user.id})")
        except Exception as e:
            print("Failed to sync command tree:", e)

        print(f"Logged in as {self.bot.user}")

    def run(self, token: str | None = None, test_run: bool = False):
        if test_run: 
            TOKEN = os.environ.get("DISCORD_TEST_TOKEN")
        else:
            TOKEN = os.environ.get("DISCORD_TOKEN")
        token = token or self.token or TOKEN
        if not token:
            raise RuntimeError("Discord token not provided")

        # Set env var so config functions use test_configs when running tests
        if test_run:
            os.environ["VC_CONTROL_TESTING"] = "1"
        else:
            os.environ.pop("VC_CONTROL_TESTING", None)
        # If a DATABASE_URL is provided, ensure DB table exists before loading
        if os.environ.get("DATABASE_URL"):
            try:
                import db as _db
                _db.ensure_table()
            except Exception as e:
                print("DB initialization failed:", e)

        load_config()
        self.bot.run(token)

start_in_test = os.getenv("TEST_MODE", "false").lower() == "true"
if start_in_test:
    print("Starting in test mode...")
    test_run = True
else:
    print("Starting in production mode...")
    test_run = False
vc_bot = VCControl()
vc_bot.run(test_run=test_run)
