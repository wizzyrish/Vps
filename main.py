import os
import json
import websockets
import asyncio
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from dotenv import load_dotenv

load_dotenv()

class TerminalBot:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.authorized_users = [int(id) for id in os.getenv("AUTHORIZED_USERS", "").split(",") if id]
        self.working_dir = os.getenv("WORKING_DIR", os.getcwd())
        self.ws_url = "ws://localhost:8765"
        self.active_tasks = {}

    async def is_authorized(self, update: Update) -> bool:
        return update.effective_user.id in self.authorized_users

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.is_authorized(update):
            return
        
        help_text = """
🖥️ Real-Time Terminal Bot

Features:
- Real-time command output streaming
- File upload/download support
- Interactive command execution

Commands:
/run <command> - Execute with real-time output
/cancel - Stop current command
/files - List available files
"""
        await update.message.reply_text(help_text)

    async def stream_command_output(self, update: Update, command: str):
        try:
            async with websockets.connect(self.ws_url) as websocket:
                await websocket.send(json.dumps({
                    'type': 'execute',
                    'command': command
                }))

                message = await update.message.reply_text("🚀 Command started...")
                full_output = []

                async for response in websocket:
                    data = json.loads(response)
                    
                    if data['type'] in ('stdout', 'stderr'):
                        output = data['output']
                        full_output.append(output)
                        
                        # Update message every 10 lines or when significant output arrives
                        if len(full_output) % 10 == 0 or "%" in output:
                            await message.edit_text(f"```\n{''.join(full_output[-200:])}\n```", 
                                                 parse_mode="Markdown")
                    
                    elif data['type'] == 'exit':
                        final_output = ''.join(full_output)
                        status = "✅ Success" if data['code'] == 0 else f"❌ Failed (code: {data['code']})"
                        await message.edit_text(
                            f"{status}\n```\n{final_output[-4000:]}\n```",
                            parse_mode="Markdown"
                        )
                        break

        except Exception as e:
            await message.edit_text(f"❌ Connection error: {str(e)}")

    async def run_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.is_authorized(update):
            return

        if not context.args:
            await update.message.reply_text("Usage: /run <command>")
            return

        command = ' '.join(context.args)
        task = asyncio.create_task(self.stream_command_output(update, command))
        self.active_tasks[update.effective_user.id] = task

        try:
            await task
        except asyncio.CancelledError:
            await update.message.reply_text("Command cancelled")
        finally:
            self.active_tasks.pop(update.effective_user.id, None)

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.is_authorized(update):
            return

        user_id = update.effective_user.id
        if user_id in self.active_tasks:
            self.active_tasks[user_id].cancel()
            await update.message.reply_text("⏹ Command cancellation requested")
        else:
            await update.message.reply_text("No active command to cancel")

    def setup_handlers(self, application):
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("run", self.run_command))
        application.add_handler(CommandHandler("cancel", self.cancel_command))
        # Add other handlers...

    async def run(self):
        application = Application.builder().token(self.token).build()
        self.setup_handlers(application)
        await application.run_polling()

if __name__ == "__main__":
    bot = TerminalBot()
    asyncio.run(bot.run())
