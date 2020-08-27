from telethon import TelegramClient, events, sync, types
from .db import DataBase
import os
import shutil
import sys
import re
import json
from datetime import datetime


class Importer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close_db()

    def __init__(self, client, datastore_folder, work_folder, display_callback=None, download_progress_callback=None, display_progress=False, create_symlink=False, check_media_ids=False):
        # Set variables
        self._client = client

        # Data store folder
        self._datastore_folder = datastore_folder
        self._media_folder = os.path.join(self._datastore_folder, 'media')
        self._chats_folder = os.path.join(self._datastore_folder, 'chats')

        # Working folder
        self._work_folder = work_folder
        self._tmp_folder = os.path.join(self._work_folder, 'tmp')

        # Make all dirs
        self._makedirs()

        self._db = self._open_db()

        # Progress related
        self._display_progress = display_progress
        self._raw_display_callback = display_callback
        self._raw_download_progress_callback = download_progress_callback

        # Create Symbolic Link
        self._create_symlink = create_symlink

        # Remove useless file(media id larger than max_media_id)
        if check_media_ids:
            self._remove_file_with_invalid_media_id()

        # Check and update personal info
        self.update_personal_info()

    def _open_db(self):
        # File paths
        datastore_db_path = os.path.join(self._datastore_folder, 'telegram_datamanager.db')
        datastore_dblock_path = os.path.join(self._datastore_folder, 'telegram_datamanager.dblock')
        datastore_json_path = os.path.join(self._datastore_folder, 'telegram_datamanager.json')
        work_db_path = os.path.join(self._work_folder, 'telegram_datamanager.db')
        work_json_path = os.path.join(self._work_folder, 'telegram_datamanager.json')

        # Check dblock in datastore folder
        dblock_exist = os.path.exists(datastore_dblock_path)
        # Check db file in work folder
        dbfile_in_workfolder = os.path.exists(work_db_path)

        if dblock_exist and not dbfile_in_workfolder:
            print('Error: dblock exists at {} but dbfile is not at {}. Please check what is going on'.format(datastore_dblock_path, work_db_path), file=sys.stderr)
            sys.exit(-1)
        elif not dblock_exist and dbfile_in_workfolder:
            print('Error: dblock is not at {} but dbfile exists at {}. Please check what is going on'.format(datastore_dblock_path, work_db_path), file=sys.stderr)
            sys.exit(-1)
        if dblock_exist and dbfile_in_workfolder:
            # Check if dir and starttime match if so open this file and return
            with open(datastore_json_path) as f:
                datastore_json = json.load(f)
            with open(work_json_path) as f:
                work_json = json.load(f)
            if not datastore_json == work_json:
                print('Error: json file in datastore folder and work folder mismatch', file=sys.stderr)
                print('In datastore:', file=sys.stderr)
                print(datastore_json, file=sys.stderr)
                print('In work:', file=sys.stderr)
                print(work_json, file=sys.stderr)
                sys.exit(-1)

            # two json match, continue previous transaction
            return DataBase(work_db_path)

        # Normal situation
        # Make dblock; copy db to work folder; write json to both dir
        open(datastore_dblock_path, 'x').close()
        shutil.copyfile(datastore_db_path, work_db_path)
        info = {'datastore_folder': os.path.realpath(self._datastore_folder),
                'work_folder': os.path.realpath(self._work_folder),
                'start_time': datetime.now().strftime('%Y-%m-%d %H-%M-%S')
                }
        with open(datastore_json_path, 'x') as f:
            json.dump(info, f)
        with open(work_json_path, 'x') as f:
            json.dump(info, f)

        return DataBase(work_db_path)

    def _close_db(self):
        # File paths
        datastore_db_path = os.path.join(self._datastore_folder, 'telegram_datamanager.db')
        datastore_dbbackup_path = os.path.join(self._datastore_folder, 'telegram_datamanager.db.backup')
        datastore_dblock_path = os.path.join(self._datastore_folder, 'telegram_datamanager.dblock')
        datastore_json_path = os.path.join(self._datastore_folder, 'telegram_datamanager.json')
        work_db_path = os.path.join(self._work_folder, 'telegram_datamanager.db')
        work_json_path = os.path.join(self._work_folder, 'telegram_datamanager.json')

        # Close db file
        self._db.close()

        # Make db backup if exist in datastore; copy db from work folder
        if os.path.exists(datastore_db_path):
            shutil.copyfile(datastore_db_path, datastore_dbbackup_path)
        shutil.copyfile(work_db_path, datastore_db_path)

        # Remove dblock; Remove json from both dir and the empty work dir
        os.remove(datastore_dblock_path)
        os.remove(datastore_json_path)
        shutil.rmtree(self._work_folder)

    def enable_progress(self):
        self._display_progress = True

    def disable_progress(self):
        self._display_progress = False

    def _display_callback(self, line0=None, line1=None, line2=None):
        if not self._display_progress:
            return
        if not self._raw_display_callback:
            return
        self._raw_display_callback(line0, line1, line2)

    def _download_progress_callback(self, recieved_bytes, total_bytes):
        if not self._display_progress:
            return
        if not self._raw_download_progress_callback:
            return
        self._raw_download_progress_callback(recieved_bytes, total_bytes)

    def _remove_file_with_invalid_media_id(self):
        self._display_callback('Removing media file with invalid id')
        # Delete all files with media_id larger than media id in the DB
        dirs = os.listdir(self._media_folder)
        for i, f in enumerate(dirs):
            self._display_callback(None, '{}/{} Working on folder: {}'.format(i+1, len(dirs), f))
            chat_folder = os.path.join(self._media_folder, f)
            if not os.path.isdir(chat_folder):
                continue
            try:
                # Convert chat_id to int
                chat_id = int(f)
            except ValueError:
                continue
            # Check if chat_id is a valid chat in DB
            if not self._db.chat_exist(chat_id):
                continue
            # Get max_media_id
            max_media_id = self._db.chat_get_media_id(chat_id)
            # Remove all invalid files
            files_to_remove = []
            files = os.listdir(chat_folder)
            for idx, ff in enumerate(files):
                if idx % 100 is 0:
                    self._display_callback(None, None, 'Checking files {}/{}'.format(idx, len(files)))
                filepath = os.path.join(chat_folder, ff)
                if not os.path.isfile(filepath):
                    continue
                match = re.fullmatch(r'^([0-9]+)\@.+$', ff)
                if match and int(match.group(1)) > max_media_id:
                    files_to_remove.append(filepath)

            for idx, ff in enumerate(files_to_remove):
                if idx % 100 is 0:
                    self._display_callback(None, None, 'Removing files {}/{}'.format(idx, len(files)))
                # TODO: change back before commit
                print('removed', file=sys.stderr)
                # os.remove(ff)

    def _makedirs(self):
        # Make dirs
        os.makedirs(self._datastore_folder, mode=0o755, exist_ok=True)
        os.makedirs(self._media_folder, mode=0o755, exist_ok=True)
        os.makedirs(self._chats_folder, mode=0o755, exist_ok=True)

        os.makedirs(self._work_folder, mode=0o755, exist_ok=True)
        # Delete and remake tmp dir
        if os.path.exists(self._tmp_folder):
            shutil.rmtree(self._tmp_folder)
        os.makedirs(self._tmp_folder, mode=0o755, exist_ok=True)

    def update_personal_info(self):
        self._display_callback('Updating personal info')
        me = self._client.get_me()

        if not me:
            raise ValueError('get_me returned null, check if client is logged in')

        user_id = me.id
        first = me.first_name
        last = me.last_name
        phone = me.phone
        username = me.username

        self._db.check_and_update_personal_info(user_id, first, last, phone, username)
        self._db.commit()

    def update_profile_photo(self):
        # TODO: relax goal
        pass

    def _match_chat_against_list(self, chat, compared_list):
        return int(chat.id) in compared_list or chat.name in compared_list

    def _get_filtered_chat(self, allow_list=None, block_list=None):
        self._display_callback(None, 'Filtering Chat')
        all_chats = self._client.get_dialogs(limit=None)

        filtered = []

        if allow_list and len(allow_list) is not 0:
            for chat in all_chats:
                if self._match_chat_against_list(chat, allow_list):
                    filtered.append(chat)
        else:
            for chat in all_chats:
                filtered.append(chat)

        if block_list and len(block_list) is not 0:
            filtered = [chat for chat in filtered if not self._match_chat_against_list(chat, block_list)]

        filtered.sort(key=lambda e: (e.name))

        return filtered

    def _get_chat_typestr(self, chat):
        if chat.is_user:
            return 'user'
        if chat.is_channel:
            return 'channel'
        return 'group'

    def _get_undownloaded_message_stat(self, chat_id, max_message_id):
        self._display_callback(None, 'Updating undownloaded message count')

        count = 0
        total_bytes = 0

        for message in self._client.iter_messages(chat_id, reverse=True, min_id=max_message_id):
            count += 1
            total_bytes += self._get_media_size_in_message(message)

            self._display_callback(None, 'Updating undownloaded message count: {:,}. Total file size: {:,}'.format(count, total_bytes))

        self._undownloaded_messages = count
        self._undownloaded_file_bytes = total_bytes

    def _get_actionstr(self, message):
        return type(message.action).__name__ if message.action else None

    def _move_media_file(self, chat_id, filename, media_ids):
        if not filename:
            return

        self._display_callback(None, None, 'Saving media file {} '.format(os.path.basename(filename)))

        # Allocate media_id
        media_id = self._db.chat_get_next_media_id(chat_id)
        # Rename and move file
        filename = os.path.basename(filename)
        new_filename = '{}@{}'.format(media_id, filename)
        old_path = os.path.join(self._tmp_folder, filename)
        new_path = os.path.join(self._media_folder, str(chat_id), new_filename)
        shutil.move(old_path, new_path, copy_function=shutil.copyfile)
        # Create Media entry to db
        self._db.media_add(chat_id, media_id, new_path)
        # Update previous_entry
        if media_ids['prev'] is not 0:
            self._db.media_update_next(chat_id, media_ids['prev'], media_id)

        if media_ids['first'] == 0:
            media_ids['first'] = media_id
        media_ids['prev'] = media_id

    def _get_media_size_in_message(self, message):
        total_bytes = 0

        if isinstance(message.media, types.MessageMediaPhoto):
            total_bytes += message.media.photo.sizes[-1].size

        if isinstance(message.media, types.MessageMediaDocument):
            total_bytes += message.media.document.size

        if isinstance(message.media, types.MessageMediaWebPage):
            webpage = message.media.webpage
            if isinstance(webpage, types.WebPage):
                if webpage.photo:
                    if isinstance(webpage.photo.sizes[-1], types.PhotoSize):
                        total_bytes += webpage.photo.sizes[-1].size
                if webpage.document:
                    total_bytes += webpage.document.size

        if message.web_preview:
            if message.web_preview.cached_page:
                # Download all photos
                if message.web_preview.cached_page.photos:
                    for photo in message.web_preview.cached_page.photos:
                        total_bytes += photo.sizes[-1].size

                # Download all documents
                if message.web_preview.cached_page.documents:
                    for doc in message.web_preview.cached_page.documents:
                        total_bytes += doc.size

        return total_bytes

    def _save_all_media(self, chat_id, message):
        # Create dir for media folder
        curr_chat_folder = os.path.join(self._media_folder, str(chat_id))
        os.makedirs(curr_chat_folder, mode=0o755, exist_ok=True)

        # Download everything
        media_ids = {'first': 0, 'prev': 0}
        # Step 1: media in document
        filename = message.download_media(file=self._tmp_folder, progress_callback=self._download_progress_callback)
        self._move_media_file(chat_id, filename, media_ids)

        if message.web_preview:
            if message.web_preview.cached_page:
                # Download all photos
                if message.web_preview.cached_page.photos:
                    for photo in message.web_preview.cached_page.photos:
                        filename = message.client.download_media(photo, file=self._tmp_folder, progress_callback=self._download_progress_callback)
                        self._move_media_file(chat_id, filename, media_ids)

                # Download all documents
                if message.web_preview.cached_page.documents:
                    for doc in message.web_preview.cached_page.documents:
                        filename = message.client.download_media(doc, file=self._tmp_folder, progress_callback=self._download_progress_callback)
                        self._move_media_file(chat_id, filename, media_ids)

        return media_ids['first'] if media_ids['first'] is not 0 else None

    def _create_symlink_for_chat(self, chat_id, chat_name):
        # Remove all folder ends with chat_id
        for f in os.listdir(self._chats_folder):
            matched = re.search(r"@(-?[0-9]+$)", f)

            if not matched:
                continue

            if int(matched.group(1)) == chat_id:
                folder_path = os.path.join(self._chats_folder, f)
                if os.path.islink(folder_path):
                    os.remove(folder_path)
                else:
                    os.rmdir(folder_path)

        # Create folder
        src_folder_path = os.path.join(self._media_folder, str(chat_id))
        dst_folder_path = os.path.join(self._chats_folder, '{}@{}'.format(chat_name, chat_id))
        if self._create_symlink:
            os.symlink(os.path.relpath(src_folder_path, start=self._chats_folder), dst_folder_path, target_is_directory=True)
        else:
            os.mkdir(dst_folder_path, mode=0o755)

    def update_chats(self, allow_list=None, block_list=None):
        self._display_callback('Updating Chats ...')
        # Step 1 filter chat
        filtered_chat = self._get_filtered_chat(allow_list, block_list)

        # Counters
        chat_total = len(filtered_chat)
        chat_count = 1

        # Save all messages in all chats
        for chat in filtered_chat:
            self._display_callback('Updating Chat {} {:,}/{:,}'.format(chat.name, chat_count, chat_total))
            chat_count += 1
            # Check if chat is in db, if not, create it
            if not self._db.chat_exist(chat.id):
                self._db.chat_add(chat.id, chat.name, self._get_chat_typestr(chat))
                self._db.commit()

            # Get max downloaded message id
            max_message_id = self._db.chat_get_max_id(chat.id)

            # Calculate # of new messages and size of all medias
            self._get_undownloaded_message_stat(chat.id, max_message_id)

            count = 1
            # Save all message in this chat
            for message in self._client.iter_messages(chat.id, reverse=True, min_id=max_message_id):
                self._display_callback(None, 'Saving message {:,}/{:,}. Total bytes remaining: {:,}'.format(count, self._undownloaded_messages, self._undownloaded_file_bytes))
                # TODO: print bytes remaining
                count += 1
                # Save one message
                # Save metadata
                self._db.message_add(
                    chat_id=chat.id,
                    message_id=message.id,
                    message_type='service' if self._get_actionstr(message) else 'message',
                    date=message.date,
                    text=message.raw_text,
                    grouped_id=message.grouped_id,
                    edited=message.edit_date,
                    sender_id=message.from_id,
                    reply_to_message_id=message.reply_to_msg_id,
                    fwd_from=message.fwd_from.channel_id if message.fwd_from else None
                )
                # Check if media is present, if so, save it
                media_id = self._save_all_media(chat.id, message)
                if media_id:
                    self._db.message_update_media_id(chat.id, message.id, media_id)

                # Update max_message_id
                max_message_id = message.id
                self._db.chat_update_max_id(chat.id, max_message_id)

                self._db.commit()

            self._create_symlink_for_chat(chat.id, chat.name)

    def estimate_chats(self,  allow_list=None, block_list=None):
        # Backup original display settings
        display_progress_backup = self._display_progress
        self._display_progress = False

        # filter chat
        filtered_chat = self._get_filtered_chat(allow_list, block_list)

        # Counters
        chat_total = len(filtered_chat)
        chat_count = 0

        # Save all messages in all chats
        for chat in filtered_chat:
            chat_count += 1

            # Get max downloaded message id
            if self._db.chat_exist(chat.id):
                max_message_id = self._db.chat_get_max_id(chat.id)
            else:
                max_message_id = 0

            sys.stdout.write('{}/{} {} Since message id:{} '.format(chat_count, chat_total, chat.name, max_message_id))
            sys.stdout.flush()

            try:
                # Calculate # of new messages and size of all medias
                self._get_undownloaded_message_stat(chat.id, max_message_id)

                print('Messages:{:,} Size of files:{:,}'.format(self._undownloaded_messages, self._undownloaded_file_bytes))
            except:
                print('Warning: Error Occurred')

        # Restore display settings:
        self._display_progress = display_progress_backup

    def print_chat_info(self, allow_list=None, block_list=None):
        # Backup original display settings
        display_progress_backup = self._display_progress
        self._display_progress = False

        # filter chat
        filtered_chat = self._get_filtered_chat(allow_list, block_list)

        # Counters
        chat_total = len(filtered_chat)
        chat_count = 0

        # Save all messages in all chats
        for chat in filtered_chat:
            chat_count += 1

            sys.stdout.write('{}/{} {} '.format(chat_count, chat_total, chat.name))
            sys.stdout.flush()

            print('id:{}'.format(chat.id))

        # Restore display settings:
        self._display_progress = display_progress_backup
