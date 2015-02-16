__author__ = 'scornflakes'
#!/usr/bin/env python2

import datetime
import hashlib
import ConfigParser
import socket
import os
import sys
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

COMPUTER_NAME = socket.gethostname()
PATH_SEP = os.path.sep

usr_home = os.getenv('USERPROFILE')
if not usr_home:
    usr_home = os.path.expanduser('~/')
settings_dir = os.path.join(usr_home, '.smartdedupe')
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

config = ConfigParser.ConfigParser()
config.read(settings_file)
db_engine = config.get("Database", 'db_engine')
print db_engine

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

    def __init__(self, computer_id, parent_id, full_path):
        global PATH_SEP
        self.computer_id = computer_id
        self.parent_id=parent_id
        path_parts = full_path.split(PATH_SEP)
        self.folder_name = path_parts[-1]
        # path = path_parts[0:-1]

        self.path = full_path
        #self.shrunk_path = ("{%d}" % self.folder_id) +PATH_SEP+self.folder_name


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
    folder = None

    def __init__(self, computer_id, folder_id, path, file_name):
        self.computer_id = computer_id
        self.file_name = file_name
        if self.folder_id != -1:
            self.folder_id = folder_id
        self.path = path
        full_path = self.get_full_path()
        self.md5_hash = md5(full_path)
        self.last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(full_path))
        self.last_checked = datetime.datetime.now()

    def get_full_path(self):
        global PATH_SEP
        return PATH_SEP.join((self.path, self.file_name))

    def check_exists(self):
        self.last_checked = datetime.datetime.now()
        return os.path.exists(self.get_full_path())



def get_computer_id():
    global COMPUTER_NAME
    computer = s.query(Computer).filter(Computer.computer_name == COMPUTER_NAME).first()
    if not computer:
        computer = Computer(COMPUTER_NAME)
        s.add(computer)
        s.commit()
    computer_id = computer.computer_id
    return computer_id


def populate_db(computer_id, root_folder):
    entries = os.listdir(root_folder.get_full_path())
    for entry in entries:
        print entry
        entry_full_path = PATH_SEP.join((root_folder.get_full_path(), entry))
        try:
            if os.path.isdir(entry_full_path) and os.access(entry_full_path, os.R_OK):

                folder = Folder(computer_id, root_folder.folder_id, entry_full_path)
                matching_folder = s.query(Folder).filter(Folder.path == folder.path).first()
                if matching_folder:
                    folder = matching_folder
                else:
                    s.add(folder)
                    s.commit()

                populate_db(computer_id, folder)

            elif os.path.isfile(entry_full_path) and os.access(entry_full_path,os.R_OK):
                file2 = File(computer_id, root_folder.folder_id, root_folder.get_full_path(),  entry)
                matching_file = s.query(File)\
                    .filter(File.folder_id == file2.folder_id, File.file_name == file2.file_name).first()
                if matching_file:
                    file2 = matching_file
                else:
                    s.add(file2)
                    s.commit()
        except WindowsError:
            pass


def remove_copies_in_same_directory(path):

    print 'to remove same dir:'
    from os.path import join
    for root, dirs, files in os.walk(path):
        for f in files:
            file1 = File(get_computer_id(), -1, root, f)
            existing_copy = s.query(File)\
                .filter(File.md5_hash == file1.md5_hash)\
                .filter(File.path == file1.path)\
                .filter(File.file_name != file1.file_name)\
                .first()
            if existing_copy:
                print file1.FullPath, existing_copy.FullPath,
                print file1.md5_hash, existing_copy.MD5Hash


def prune(directory,  to_delete=False):
### does not identify copies in same dir!!

    print 'to prune:'
    from os.path import join
    for root, dirs, files in os.walk(directory):
        for f in files:

            file = File(get_computer_id(), -1,  root, f)
            existingcopy = s.query(File).filter(File.md5_hash== file.md5_hash).filter(File.FullPath != file.path).first()
            if existingcopy and (directory not in existingcopy.FullPath):
                print file.FullPath, existingcopy.FullPath,
                print file.md5_hash, existingcopy.MD5Hash
                if to_delete:
                    os.remove(file.get_full_path())

def main(argv):

    root_folder = Folder(get_computer_id(), -1, 'D:\\MATLAB_stuff')
    #s.add(root_folder)
    #s.commit()
    populate_db(get_computer_id(), root_folder)



Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
s = Session()


if __name__ == "__main__":
    main(sys.argv[1:])

