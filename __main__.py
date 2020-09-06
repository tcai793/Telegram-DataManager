from telegram_datamanager.importer import Importer
import json
from telethon import TelegramClient, sync
from telegram_datamanager.progress import Progress
from telegram_datamanager.folders import Folders

if __name__ == '__main__':
    # Parse config
    config = {}
    with open('config.json') as config_fp:
        config = json.load(config_fp)

    api_id = config['api_id']
    api_hash = config['api_hash']
    folder_names = config['folder_names']
    chat_names = config['chat_names']
    datastore_folder = config['datastore_folder']
    work_folder = config['work_folder']
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

    chats_from_folder = Folders(client).get_dialog_ids_from_folder(folder_names)

    chat_names = chat_names + chats_from_folder

    with Importer(client, datastore_folder, work_folder, display_callback, download_progress_callback, True) as im:
        im.update_chats(allow_list=chat_names)
