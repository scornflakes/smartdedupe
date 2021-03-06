#!/usr/bin/env python3
__author__ = 'scornflakes'
import datetime
import hashlib
import configparser
import socket
import os
import argparse

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


COMPUTER_NAME = socket.gethostname()
PATH_SEP = os.path.sep

usr_home = os.getenv('USERPROFILE')
if not usr_home:
    usr_home = os.path.expanduser('~/')
settings_dir = os.path.join(usr_home, '.smartdedupe')
print('settings dir', settings_dir)
settings_file = os.path.join(settings_dir, 'settings.ini')
db_location = os.path.join(settings_dir, 'smartdedupe.db')
# make default directory and files if do not exist
if not os.path.exists(settings_dir):
    os.makedirs(settings_dir)
if not os.path.exists(settings_file):
    file1 = open(settings_file, 'w')
    file1.write("""[Database]
db_engine: sqlite:///{}
""".format(db_location))
    file1.close()

config = configparser.ConfigParser()
config.read(settings_file)
db_engine = config.get("Database", 'db_engine')

engine = sqlalchemy.create_engine(db_engine)
Base = declarative_base(bind=engine)


def md5string(str):
    import hashlib

    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()


def md5(file_path):
    try:
        f = open(file_path, 'rb')
        m = hashlib.md5()
        while True:
            data = f.read(10240)
            if len(data) == 0:
                break
            m.update(data)
        f.close()
        return m.hexdigest()
    except IOError:
        return None


# http://akiscode.com/articles/sha-1directoryhash.shtml


class Computer(Base):
    __tablename__ = 'computers'
    computer_id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, primary_key=True)
    computer_name = sqlalchemy.Column(sqlalchemy.String(270), nullable=False)

    def __init__(self, computer_name):
        self.computer_name = computer_name


class Folder(Base):
    __tablename__ = 'folders'
    folder_id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, primary_key=True)
    folder_name = sqlalchemy.Column(sqlalchemy.String(270), nullable=False)
    path = sqlalchemy.Column(sqlalchemy.String(800), nullable=False)
    shrunk_path = sqlalchemy.Column(sqlalchemy.String(270))
    path_is_truncated = sqlalchemy.Column(sqlalchemy.Boolean)
    parent_id = sqlalchemy.Column(sqlalchemy.Integer)
    computer_id = sqlalchemy.Column(sqlalchemy.String(20))
    last_modified = sqlalchemy.Column(sqlalchemy.DateTime)
    last_checked = sqlalchemy.Column(sqlalchemy.DateTime)
    md5_hash = sqlalchemy.Column(sqlalchemy.String(32), nullable=True)

    def __init__(self, full_path, parent_id=-1):
        global PATH_SEP
        self.computer_id = get_computer_id()
        self.parent_id = parent_id
        path_parts = full_path.split(PATH_SEP)
        self.folder_name = path_parts[-1]
        # path = path_parts[0:-1]

        self.path = full_path
        self.shrunk_path = ("{%d}" % self.parent_id) + PATH_SEP + self.folder_name

    def get_full_path(self):
        global PATH_SEP
        if self.path_is_truncated:
            return PATH_SEP.join(s.query(Folder).get(self.parent_id).get_full_path(), self.folder_name)
        else:
            return self.path


class File(Base):
    __tablename__ = 'files'
    file_id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, primary_key=True)
    file_name = sqlalchemy.Column(sqlalchemy.String(270), nullable=False)
    path = sqlalchemy.Column(sqlalchemy.String(800), nullable=False)
    computer_id = sqlalchemy.Column(sqlalchemy.String(20), nullable=False)
    folder_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    md5_hash = sqlalchemy.Column(sqlalchemy.String(32), nullable=True)
    last_modified = sqlalchemy.Column(sqlalchemy.DateTime)
    last_checked = sqlalchemy.Column(sqlalchemy.DateTime)
    is_deleted = sqlalchemy.Column(sqlalchemy.Boolean)
    size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    folder = None

    def __init__(self, path, file_name, folder_id=-1):
        self.computer_id = get_computer_id()
        self.file_name = file_name
        self.folder_id = folder_id
        self.path = path
        full_path = self.get_full_path()

        try:
            self.last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(full_path))
            self.size = os.path.getsize(full_path)
            self.md5_hash = md5(full_path)
        except:
            self.last_modified = 0
            self.size = 0

        self.last_checked = datetime.datetime.now()
        self.is_deleted = False


    def get_full_path(self):
        global PATH_SEP
        return PATH_SEP.join((self.path, self.file_name))

    def check_exists(self):
        self.last_checked = datetime.datetime.now()
        return os.path.exists(self.get_full_path())

    def update(self):
        full_path = self.get_full_path()
        if os.path.exists(full_path):
            self.is_deleted = False
            new_last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(full_path))
            if self.last_modified != new_last_modified:
                self.md5_hash = md5(full_path)
                self.last_modified=new_last_modified
            if self.size == 0:
                self.size = os.path.getsize(full_path)
            self.last_checked= datetime.datetime.now()
        else:
            print(full_path + " is deleted")
            self.is_deleted = True


        s.commit()

    def delete(self):
        fullpath = self.get_full_path()
        if os.path.exists(self.get_full_path()):
            try:
                os.remove(fullpath)
                self.is_deleted = True
                print('deleted!')
            except Exception:
                print('could not delete...')
        else:
            print("already removed...")
        # s.delete(self)
        s.add(self)
        s.commit()


def get_computer_id():
    global COMPUTER_NAME
    computer = s.query(Computer).filter(Computer.computer_name == COMPUTER_NAME).first()
    if not computer:
        computer = Computer(COMPUTER_NAME)
        s.add(computer)
        s.commit()
    computer_id = computer.computer_id
    return computer_id


def populate_db(root_folder):
    entries = os.listdir(root_folder.get_full_path())

    for entry in entries:
        print(entry)
        path = root_folder.get_full_path()
        entry_full_path = PATH_SEP.join((path, entry))
        try:
            if os.path.isdir(entry_full_path) and os.access(entry_full_path, os.R_OK):
                folder = get_or_create_folder(entry_full_path)
                populate_db(folder)

            elif os.path.isfile(entry_full_path) and os.access(entry_full_path, os.R_OK):
                get_or_create_file(path, entry)

        except Exception:
            pass


def remove_neighbor_dupes(path, to_delete=False, verbose_mode=True):
    print('to remove same dir:')
    for root, dirs, files in os.walk(path):
        for f in files:
            file1 = get_or_create_file(root, f)
            existing_copy = s.query(File) \
                .filter(File.md5_hash == file1.md5_hash) \
                .filter(File.path == file1.path) \
                .filter(File.file_name != file1.file_name) \
                .filter(File.is_deleted == False) \
                .first()
            if existing_copy:
                if verbose_mode:
                    print(repr(file1.get_full_path()), repr(existing_copy.get_full_path()), )
                    print(file1.md5_hash, existing_copy.md5_hash)
                if to_delete:
                    file1.delete()


def kill_from_pc(directory, to_delete=False, verbose_mode=True):
    print('to delete files on this pc that exist on another pc')
    print('delete?' + repr(to_delete))
    print('to kill:', directory)
    directory = directory.replace('\\','\\\\')+"%"
    print(repr(directory))
    files = s.query(File)\
        .filter(File.path.like(directory))\
        .filter(File.is_deleted == False) \
        .filter(File.size != 0)\
        .filter(File.computer_id==get_computer_id())\
        .all()
    if len(files)==0:
        print("No files found in specified directory!")
    else:
        print('I found a total of {} files in this directory'.format(len(files)))
    for file2 in files:
        existing_copy = s.query(File) \
            .filter(File.is_deleted == False)\
            .filter(File.computer_id != file2.computer_id)\
            .filter(File.md5_hash == file2.md5_hash) \
            .first()
        if existing_copy:
            if verbose_mode:
                print(repr(file2.get_full_path()), repr(existing_copy.get_full_path()),)
                print(file2.md5_hash, existing_copy.md5_hash)
            if to_delete:
                file2.delete()
    remove_empty_folders(directory)


def prune(directory, to_delete=False, verbose_mode=True):
    ### does not identify copies in same dir!!

    print('to prune:', directory)
    directory_match = directory.replace('\\', '\\\\') + "%"

    print(repr(directory_match))
    files = s.query(File)\
        .filter(File.path.like(directory_match))\
        .filter(File.is_deleted == False) \
        .filter(File.computer_id==get_computer_id())\
        .filter(File.size != 0)\
        .all()
    total_count = len(files)
    i = 0
    total_size = 0
    if len(files)==0:
        print("No files found in specified directory!")
    else:
        print('I found a total of {} files in this directory'.format(len(files)))

    for file2 in files:
        i += 1
        existing_copy = s.query(File) \
            .filter(File.is_deleted == False)\
            .filter(File.computer_id == file2.computer_id)\
            .filter(File.md5_hash == file2.md5_hash) \
            .filter(~ File.path.like(directory_match))\
            .filter(File.path != file2.path).first()
        if existing_copy:
            if verbose_mode:
                print(repr(file2.get_full_path()), repr(existing_copy.get_full_path()),)
                print(file2.md5_hash, existing_copy.md5_hash, file2.size)
                total_size += file2.size
            if to_delete:
                file2.delete()
        else:
            if verbose_mode and (i % 5) == 0:
                print("no copy...{}/{}".format(i, total_count))
    remove_empty_folders(directory)
    print(" I removed {} bytes worth of data in {} files".format(total_size, total_count))


def get_or_create_folder(path):
    folder = s.query(Folder).filter(Folder.path == path).first()
    if not folder:
        folder = Folder(path)
        s.add(folder)
        s.commit()
    return folder


def get_or_create_file(path, file_name):
    file1 = s.query(File).filter(File.file_name == file_name, File.path == path).first()
    if not file1:
        file1 = File(path, file_name)
        s.add(file1)
    else:
        file1.update()
    s.add(file1)
    s.commit()
    return file1

def remove_empty_folders(path):
  if not os.path.isdir(path):
    return

  # remove empty subfolders
  files = os.listdir(path)
  if len(files):
    for f in files:
      fullpath = os.path.join(path, f)
      if os.path.isdir(fullpath):
        remove_empty_folders(fullpath)

  # if folder empty, delete it
  files = os.listdir(path)
  if len(files) == 0:
    print("Removing empty folder:", path)
    os.rmdir(path)

def main():
    parser = argparse.ArgumentParser(description="find duplicates in folders")
    parser.add_argument("-d", "--delete", action='store_true')
    parser.add_argument("-v", "--verbose", action='store_true')
    parser.add_argument("-s", "--scan_dirs", action='append')
    parser.add_argument("-u", "--update_dirs", action='append')
    parser.add_argument("-l", "--list_dupes", action='append')
    parser.add_argument("-n", "--list_neighbors", action='append')
    parser.add_argument("-k", "--kill_from_pc", action='append')
    parser.add_argument("-p", "--print_files", action='append')
    parser.add_argument("-r", "--remove-from-db", action='append')
    args = parser.parse_args()

    if not args.verbose:
        print("verbose mode=off")
    if args.print_files:
        for dir in args.print_files:
            files = s.query(File)\
                .filter(File.path.like(dir.replace('\\','\\\\')+"%"))\
                .filter(File.is_deleted == False) \
                .all()
            for f in files:
                print(f.file_name)

    if args.scan_dirs:
        for dir in args.scan_dirs:
            root_folder = get_or_create_folder(dir)
            current_datetime = datetime.datetime.now()
            populate_db(root_folder)
            s.commit()

    if args.update_dirs:
        current_datetime= datetime.datetime.now()
        for dir in args.update_dirs:
            for root, dirs, files in os.walk(dir):
                for f in files:
                    full_path = os.path.join(root, f)
                    print(full_path)
                    file_to_update = get_or_create_file(root, f)
            print('finding missing files...')
            missing_files = s.query(File)\
                .filter(File.is_deleted == False)\
                .filter(File.path.like(dir.replace('\\','\\\\')+"%"))\
                .filter(File.last_checked < current_datetime)\
                .filter(File.computer_id  == get_computer_id())
            for missing_file in missing_files:
                missing_file.update()
    if args.list_dupes:
        for dir in args.list_dupes:
            prune(dir, to_delete=args.delete, verbose_mode=args.verbose)
    if args.list_neighbors:
        for dir in args.list_neighbors:
            remove_neighbor_dupes(dir, to_delete=args.delete, verbose_mode=args.verbose)
    if args.kill_from_pc:
        for dir in args.kill_from_pc:
            kill_from_pc(dir, to_delete=args.delete, verbose_mode=args.verbose)
    print('done')



Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
s = Session()

if __name__ == "__main__":
    main()


