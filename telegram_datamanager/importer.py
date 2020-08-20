from telethon import TelegramClient, events, sync
from .db import DataBase
import os
import shutil
import sys
from telegram_datamanager.progress import Progress


class Importer:
    def __init__(self, client, root_folder, progress, download_progress_callback):
        self._client = client

        self._root_folder = root_folder
        os.makedirs(root_folder, mode=0o755, exist_ok=True)

        self._tmp_folder = os.path.join(self._root_folder, 'tmp')
        self._media_folder = os.path.join(self._root_folder, 'media')

        self._db = DataBase(os.path.join(root_folder, 'telegram_datamanager.db'))

        self._progress = progress
        self._download_progress_callback = download_progress_callback

        # Check and update personal info
        self.update_personal_info()

    def update_personal_info(self):
        self._progress.update_line(0, 'Updating personal info')
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
        return chat.id in compared_list or chat.name in compared_list

    def _get_filtered_chat(self, black_list, white_list):
        self._progress.update_line(1, 'Filtering Chat')
        # Match policy:
        # if white_list is not empty, remove everything that does not in it
        # if black_list is not empty, remove everything that is in it
        all_chats = self._client.get_dialogs(limit=None)

        filtered = []

        if len(white_list) is not 0:
            for chat in all_chats:
                if self._match_chat_against_list(chat, white_list):
                    filtered.append(chat)
        # TODO: currently if no white_list is supplied then no chat is saved

        if len(black_list) is not 0:
            for chat in filtered:
                if self._match_chat_against_list(chat, black_list):
                    filtered.remove(chat)

        return filtered

    def _get_chat_typestr(self, chat):
        if chat.is_user:
            return 'user'
        if chat.is_channel:
            return 'channel'
        return 'group'

    def _get_undownloaded_message_count(self, chat_id, max_message_id):
        self._progress.update_line(1, 'Updating undownloaded message count')
        count = 0
        for message in self._client.iter_messages(chat_id, reverse=True, min_id=max_message_id):
            count += 1
            self._progress.update_line(1, 'Updating undownloaded message count: {}'.format(count))
        return count

    def _get_actionstr(self, message):
        return type(message.action).__name__ if message.action else None

    def _move_media_file(self, chat_id, filename, media_ids):
        if not filename:
            return

        self._progress.update_line(2, 'Saving media file {} '.format(os.path.basename(filename)))

        # Allocate media_id
        media_id = self._db.get_next_media_id(chat_id)
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

    def _save_all_media(self, chat_id, message):
        # Rebuild tmp folder
        if os.path.exists(self._tmp_folder):
            shutil.rmtree(self._tmp_folder)
        os.makedirs(self._tmp_folder, mode=0o755, exist_ok=True)
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

    def update_chats(self, black_list=[], white_list=[]):
        self._progress.update_line(0, 'Updating Chats ...')
        # Step 1 filter chat
        filtered_chat = self._get_filtered_chat(black_list, white_list)

        # Counters
        chat_total = 0
        for chat in filtered_chat:
            chat_total += 1

        chat_count = 1

        # Save all messages in all chats
        for chat in filtered_chat:
            self._progress.update_line(0, 'Updating Chat {} {}/{}'.format(chat.name, chat_count, chat_total))
            chat_count += 1
            # Check if chat is in db, if not, create it
            if not self._db.chat_exist(chat.id):
                self._db.chat_add(chat.id, chat.name, self._get_chat_typestr(chat))
                self._db.commit()

            max_message_id = self._db.chat_get_max_id(chat.id)
            total_undownloaded = self._get_undownloaded_message_count(chat.id, max_message_id)
            count = 1

            # Save all message in this chat
            for message in self._client.iter_messages(chat.id, reverse=True, min_id=max_message_id):
                self._progress.update_line(1, 'Saving message {}/{}'.format(count, total_undownloaded))
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
