import sqlite3
import os
from datetime import datetime


class DataBase:
    def _create(self, filename):
        if os.path.exists(filename):
            raise OSError("filename {} already exist".format(filename))

        os.makedirs(os.path.dirname(filename), mode=0o755, exist_ok=True)

        with sqlite3.connect(filename) as conn:
            c = conn.cursor()

            # Create tables
            c.execute(''' create table General (
                version text
            )''')

            c.execute(''' create table PersonalInfo (
                user_id integer,
                first text,
                last text,
                phone text,
                username text
            )''')

            c.execute(''' create table ProfilePhoto (
                date text,
                photo text
            )''')

            c.execute(''' create table Contact (
                user_id integer,
                first text,
                last text,
                phone text,
                date text
            )''')

            c.execute(''' create table Chat (
                chat_id integer primary key,
                name text,
                type text,
                max_message_id integer,
                media_count integer
            )''')

            c.execute(''' create table Message (
                message_id integer,
                chat_id integer,
                grouped_id integer,
                type text,
                date text,
                text text,

                edited text,
                sender_id text,
                reply_to_message_id integer,
                fwd_from integer,
                media_id integer,
                primary key (message_id, chat_id)
            )''')

            c.execute(''' create table Media (
                chat_id integer,
                media_id integer,
                file text,
                next_id,
                primary key (media_id, chat_id)
            )''')

            # Init fields
            c.execute('insert into General values (?)', ('V1.0.0', ))

            conn.commit()

    def __init__(self, filename):
        # Create db if
        if not os.path.exists(filename):
            self._create(filename)

        self._conn = sqlite3.connect(filename)

    def _date_to_str(self, date):
        return date.strftime('%Y-%m-%d %H:%M:%S')

    def commit(self):
        self._conn.commit()

    # Personal Info
    def check_and_update_personal_info(self, user_id, first, last, phone, username):
        c = self._conn.cursor()

        if c.execute('SELECT * from PersonalInfo').fetchone() is None:
            # Has no existing personal info, just add
            c.execute('INSERT into PersonalInfo VALUES(?,?,?,?,?)',
                      (user_id, first, last, phone, username))
            return

        # DB has existing personal info, check user_id and update
        if c.execute('SELECT user_id from PersonalInfo').fetchone()[0] != user_id:
            raise ValueError("user_id mismatch")

        c.execute('''UPDATE PersonalInfo SET 
            first = ?, 
            last = ?,
            phone = ?,
            username = ?''', (first, last, phone, username))

    # Profile Photo
    def append_profile_photo(self, date, photopath):
        c = self._conn.cursor()

        datestr = self._date_to_str(date)
        c.execute('INSERT into ProfilePhoto VALUES(?,?)', (datestr, photopath))

    # Contact
    def update_contact(self, contact_list):
        # TODO
        pass

    # Chat
    def chat_exist(self, chat_id):
        if self._conn.execute('SELECT * from Chat WHERE chat_id=?', (chat_id,)).fetchone():
            return True
        return False

    def chat_add(self, chat_id, name, chat_type):
        if self.chat_exist(chat_id):
            raise ValueError('add duplicate chat')

        self._conn.execute('INSERT into Chat VALUES(?,?,?,?,?)',
                           (chat_id, name, chat_type, 0, 0))

    def chat_get_max_id(self, chat_id):
        if not self.chat_exist(chat_id):
            raise ValueError('chat_get_max_id: chat_id DNS')

        return self._conn.execute('SELECT max_message_id from Chat WHERE chat_id=?',
                                  (chat_id,)).fetchone()[0]

    def chat_update_max_id(self, chat_id, max_message_id):
        if not self.chat_exist(chat_id):
            raise ValueError('chat_update_max_id: chat_id DNE')

        self._conn.execute('UPDATE Chat SET max_message_id=? WHERE chat_id=?', (max_message_id, chat_id))

    def get_next_media_id(self, chat_id):
        if not self.chat_exist(chat_id):
            raise ValueError('get_next_media_id: chat_id DNE')

        c = self._conn.cursor()

        curr_count = c.execute('SELECT media_count from Chat WHERE chat_id=?', (chat_id,)).fetchone()[0]
        curr_count += 1
        c.execute('UPDATE Chat SET media_count = ? WHERE chat_id=?', (curr_count, chat_id))

        return curr_count

    # Message
    def message_exist(self, chat_id, message_id):
        if self._conn.execute('SELECT * from Message WHERE chat_id=? AND message_id=?',
                              (chat_id, message_id)).fetchone():
            return True
        return False

    def message_add(self, chat_id, message_id, message_type, date, text, grouped_id=0, edited=None, sender_id=None, reply_to_message_id=None, fwd_from=None, media_id=None):
        if self.message_exist(chat_id, message_id):
            raise ValueError('add duplicate message')

        datestr = self._date_to_str(date)
        self._conn.execute('INSERT into Message VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                           (message_id, chat_id, grouped_id, message_type, datestr, text, edited, sender_id, reply_to_message_id, fwd_from, media_id))

    def message_update_media_id(self, chat_id, message_id, media_id):
        if not self.message_exist(chat_id, message_id):
            raise ValueError('update nonexist message')

        self._conn.execute('UPDATE Message SET media_id=? WHERE chat_id=? AND message_id=?', (media_id, chat_id, message_id))

    # Media
    def media_exist(self, chat_id, media_id):
        if self._conn.execute('SELECT * from Media WHERE chat_id=? AND media_id=?', (chat_id, media_id)).fetchone():
            return True
        return False

    def media_add(self, chat_id, media_id, filepath, next_id=None):
        if self.media_exist(chat_id, media_id):
            raise ValueError('add duplicate media')

        self._conn.execute('INSERT into Media VALUES(?,?,?,?)', (chat_id, media_id, filepath, next_id))

    def media_update_next(self, chat_id, media_id, next_id):
        if not self.media_exist(chat_id, media_id):
            raise ValueError('update nonexist media')

        self._conn.execute('UPDATE Media SET next_id=? WHERE chat_id=? AND media_id=?', (next_id, chat_id, media_id))
