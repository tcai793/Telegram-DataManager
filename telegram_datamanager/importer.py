from telethon import TelegramClient, events, sync, types
from .db import DataBase
import os
import shutil
import sys
import re


class Importer:
    def __init__(self, client, root_folder, db_folder, display_progress=False, display_callback=None, download_progress_callback=None):
        # Set variables
        self._client = client

        self._root_folder = root_folder

        self._tmp_folder = os.path.join(self._root_folder, 'tmp')
        self._media_folder = os.path.join(self._root_folder, 'media')

        self._db = DataBase(os.path.join(db_folder, 'telegram_datamanager.db'))

        self._display_progress = display_progress
        self._raw_display_callback = display_callback
        self._raw_download_progress_callback = download_progress_callback

        # Maintain folder structure and remove useless file
        self._maintain_structure()

        # Check and update personal info
        self.update_personal_info()

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
                os.remove(ff)

    def _maintain_structure(self):
        os.makedirs(self._root_folder, mode=0o755, exist_ok=True)

        os.makedirs(self._media_folder, mode=0o755, exist_ok=True)

        self._remove_file_with_invalid_media_id()

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
        os.rename(old_path, new_path)
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
