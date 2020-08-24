from telegram_datamanager.importer import Importer
import json
from telethon import TelegramClient, sync
from telegram_datamanager.progress import Progress

if __name__ == '__main__':
    # Parse config
    config = {}
    with open('config.json') as config_fp:
        config = json.load(config_fp)

    api_id = config['api_id']
    api_hash = config['api_hash']
    chat_names = config['chat_names']
    root_folder = config['root_folder']
    db_folder = config['db_folder']
    session_name = config['session_name']

    # CLI
    pg = Progress(3)

    def download_progress_callback(recieved, total):
        pg.update_line(2, 'Downloading File {:,}/{:,}'.format(recieved, total))

    def display_callback(line0=None, line1=None, line2=None):
        if line0:
            pg.update_line(0, line0)
            pg.update_line(1, '')
            pg.update_line(2, '')
        if line1:
            pg.update_line(1, line1)
            pg.update_line(2, '')
        if line2:
            pg.update_line(2, line2)

    # Start client
    client = TelegramClient(session_name, api_id, api_hash)
    client.start()

    im = Importer(client, root_folder, db_folder, True, display_callback, download_progress_callback)

    im.update_chats(allow_list=chat_names)
