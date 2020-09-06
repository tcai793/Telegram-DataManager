from telethon import TelegramClient, sync, functions, types, utils
from datetime import datetime


class Folder:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.peer_ids = list()

    def append(self, peer_id):
        self.peer_ids.append(peer_id)

    def __str__(self):
        return "{} {}\n{}".format(self.id, self.name, self.peer_ids)


class Folders:
    def __init__(self, client):
        self._client = client
        self.folders = list()

    def __str__(self):
        ret = ""
        for f in self.folders:
            ret += "{}\n\n".format(f.__str__())
        return ret

    def get_contact_list(self):
        contact_list = []
        result = self._client(functions.contacts.GetContactsRequest(hash=0))
        for c in result.contacts:
            contact_list.append(c.user_id)
        return contact_list

    def get_dialog_type(self, dialog):
        if isinstance(dialog.entity, types.Channel):
            if dialog.entity.megagroup:
                return 'megagroup'
            return 'channel'
        if isinstance(dialog.entity, types.Chat):
            return 'group'
        if dialog.entity.bot:
            return 'bot'
        return 'user'

    def generate_dialog_info(self, dialog, contacts):
        result = {
            'id': dialog.id,
            'is_contact': dialog.id in contacts,
            'type': self.get_dialog_type(dialog),
            'muted': False,
            'read': dialog.unread_count == 0,
            'archived': dialog.archived
        }

        mute_until = dialog.dialog.notify_settings.mute_until
        if mute_until:
            result['muted'] = mute_until.replace(tzinfo=None) > datetime.utcnow()
        return result

    def match_dialog_w_input_peer(self, dialog, input_peer):
        return dialog['id'] == utils.get_peer_id(input_peer)

    def match_dialog_w_filter(self, dialog_info, dialog_filter):
        include = False
        exclude = False

        # Check if is included manually
        for input_peer in dialog_filter.include_peers:
            if self.match_dialog_w_input_peer(dialog_info, input_peer):
                return True

        # Check if is excluded manually
        for input_peer in dialog_filter.exclude_peers:
            if self.match_dialog_w_input_peer(dialog_info, input_peer):
                return False

        # Check against rules
        if dialog_filter.contacts and dialog_info['is_contact'] and dialog_info['type'] == 'user':
            include = True
        if dialog_filter.non_contacts and not dialog_info['is_contact'] and dialog_info['type'] == 'user':
            include = True
        if dialog_filter.groups and dialog_info['type'] == 'group' or \
                dialog_filter.groups and dialog_info['type'] == 'megagroup':
            include = True
        if dialog_filter.broadcasts and dialog_info['type'] == 'channel':
            include = True
        if dialog_filter.bots and dialog_info['type'] == 'bot':
            include = True

        if dialog_filter.exclude_muted and dialog_info['muted']:
            exclude = True
        if dialog_filter.exclude_read and dialog_info['read']:
            exclude = True
        if dialog_filter.exclude_archived and dialog_info['archived']:
            exclude = True

        return include and not exclude

    def update(self):
        all_dialogs = self._client.get_dialogs(limit=None, ignore_migrated=True)
        all_contacts = self.get_contact_list()
        all_dialog_filters = self._client(functions.messages.GetDialogFiltersRequest())

        for dialog_filter in all_dialog_filters:
            folder = Folder(dialog_filter.id, dialog_filter.title)

            for dialog in all_dialogs:
                info = self.generate_dialog_info(dialog, all_contacts)
                if self.match_dialog_w_filter(info, dialog_filter):
                    folder.append(dialog.id)

            self.folders.append(folder)

    def print_folders(self):
        if self.folders == []:
            self.update()

        for folder in self.folders:
            print('{}:'.format(folder.name))
            for dialog_id in folder.peer_ids:
                print('\t{}'.format(self.get_name_from_id(dialog_id)))
            print()

    def generate_dialog_to_folder_map(self, ignored_list=None):
        if self.folders == []:
            self.update()

        all_dialogs = self._client.get_dialogs(limit=None, ignore_migrated=True)

        # Init map
        dialog_to_folder = {}
        for dialog in all_dialogs:
            dialog_to_folder[dialog.id] = []

        # Process
        for folder in self.folders:
            if folder.name in ignored_list or folder.id in ignored_list:
                continue

            for dialog_id in folder.peer_ids:
                dialog_to_folder[dialog_id].append(folder)

        return dialog_to_folder

    def get_name_from_id(self, id):
        entity = self._client.get_entity(id)

        if isinstance(entity, types.User):
            return "{} {}".format(entity.first_name, entity.last_name)

        return entity.title

    def print_dialog_to_folder_map(self, ignored_list=None):
        m = self.generate_dialog_to_folder_map(ignored_list)

        for dialog_id in m.keys():
            print("{}({}):".format(self.get_name_from_id(dialog_id), dialog_id))
            for folder in m[dialog_id]:
                print('\t', folder.name)
            print()

    def check_all_dialog_in_one_folder(self, ignored_folder_list=None):
        dtf = self.generate_dialog_to_folder_map(ignored_folder_list)
        for d in dtf.keys():
            if len(dtf[d]) != 1:
                print('Dialog {} is in {} folder(s)'.format(self.get_name_from_id(d), len(dtf[d])))
                for f in dtf[d]:
                    print('\t{}'.format(f.name))

    def get_dialog_ids_from_folder(self, include_folder_list=None):
        ids = []

        if include_folder_list is None:
            return ids

        if self.folders == []:
            self.update()

        for folder in self.folders:
            if folder.name in include_folder_list or folder.id in include_folder_list:
                for peer_id in folder.peer_ids:
                    ids.append(peer_id)

        return ids
