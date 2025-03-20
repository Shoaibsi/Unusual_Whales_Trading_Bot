"""
Telegram Webhook Handler

This module provides a simple web server to handle incoming webhook requests from Telegram.
"""

import os
import json
import asyncio
import logging
from aiohttp import web
from dotenv import load_dotenv
from loguru import logger

# Add the project root to Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ws_bot.src.telegram_notifier import TelegramNotifier

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class TelegramWebhookHandler:
    """Handles Telegram webhook requests."""
    
    def __init__(self, host='0.0.0.0', port=8443):
        """Initialize the webhook handler."""
        self.host = host
        self.port = port
        self.app = web.Application()
        self.app.add_routes([web.post('/telegram/webhook', self.handle_webhook)])
        self.telegram = None
        
    async def initialize(self):
        """Initialize the Telegram notifier."""
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not token or not chat_id:
            logger.error("Telegram credentials not provided, webhook handler will not function")
            return False
            
        self.telegram = TelegramNotifier(token, chat_id)
        return await self.telegram.initialize()
        
    async def handle_webhook(self, request):
        """Handle incoming webhook requests from Telegram."""
        try:
            # Verify the request is from Telegram
            if not self.telegram:
                return web.Response(text="Telegram notifier not initialized", status=500)
                
            # Parse the request body
            data = await request.json()
            logger.debug(f"Received webhook data: {json.dumps(data)[:200]}...")
            
            # Process the message
            await self.telegram.process_incoming_message(data)
            
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"Error handling webhook request: {str(e)}")
            return web.Response(text="Error", status=500)
            
    async def start(self):
        """Start the webhook server."""
        if not await self.initialize():
            logger.error("Failed to initialize Telegram notifier, webhook server will not start")
            return False
            
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        
        logger.info(f"Starting Telegram webhook server on {self.host}:{self.port}")
        await site.start()
        
        # Send a notification that the webhook server is running
        await self.telegram.send_system_message("Telegram webhook server is now running")
        
        return True
        
    async def set_webhook(self, webhook_url):
        """Set the webhook URL for the Telegram bot."""
        if not self.telegram:
            logger.error("Telegram notifier not initialized")
            return False
            
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        url = f"https://api.telegram.org/bot{token}/setWebhook"
        
        async with self.telegram.session.post(url, json={"url": webhook_url}) as response:
            result = await response.json()
            if result.get("ok"):
                logger.info(f"Webhook set successfully: {webhook_url}")
                return True
            else:
                logger.error(f"Failed to set webhook: {result}")
                return False

async def main():
    """Main function to run the webhook server."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Telegram Webhook Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8443, help='Port to bind the server to')
    parser.add_argument('--webhook-url', help='URL to set as the webhook for the Telegram bot')
    args = parser.parse_args()
    
    # Create and start the webhook handler
    handler = TelegramWebhookHandler(args.host, args.port)
    
    if await handler.start():
        logger.info("Webhook server started successfully")
        
        # Set the webhook URL if provided
        if args.webhook_url:
            if await handler.set_webhook(args.webhook_url):
                logger.info(f"Webhook URL set to {args.webhook_url}")
            else:
                logger.error("Failed to set webhook URL")
        
        # Keep the server running
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep for an hour
        except KeyboardInterrupt:
            logger.info("Webhook server stopped by user")
    else:
        logger.error("Failed to start webhook server")

if __name__ == "__main__":
    asyncio.run(main())
