__author__ = 'scornflakes'
#!/usr/bin/env python2

#packages required: yaml, beautifulsoup4

import datetime
import hashlib
import ConfigParser
import socket
import os
import sys
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.orm import relation, sessionmaker
from sqlalchemy.orm import  sessionmaker

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

engine = create_engine(db_engine)
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
    computer_id = Column(Integer, autoincrement=True, primary_key=True)
    computer_name = Column(String(270), nullable=False)

    def __init__(self, computer_name):
        self.computer_name = computer_name


class Folder(Base):
    __tablename__ = 'folders'
    folder_id = Column(Integer, autoincrement=True, primary_key=True)
    folder_name = Column(String(270), nullable=False)
    path = Column(String(270), nullable=False)
    path_is_truncated = Column(Boolean)
    parent_id = Column(Integer)
    computer_id = Column(String(20))
    last_modified = Column(DateTime)
    last_checked = Column(DateTime)
    md5_hash = Column(String(32), nullable=True)

    def __init__(self, computer_id, parent_id, full_path):
        global PATH_SEP
        self.computer_id = computer_id
        self.parent_id=parent_id
        path_parts = full_path.split(PATH_SEP)
        self.folder_name = path_parts[-1]
        path = path_parts[0:-1]

        self.path = "{"+parent_id+"}"+PATH_SEP+self.folder_name

        """
        if len(path) >= 270:
            s.query(Folder).filter(Folder.path)
            self.path_is_truncated = True
            self.path = "{"+parent_id+"}"+PATH_SEP+self.folder_name
        else:
            self.path=path
        """

    def get_full_path(self):
        global PATH_SEP
        if self.path_is_truncated:
            return PATH_SEP.join(s.query(Folder).get(self.parent_id).get_full_path(), self.folder_name)
        else:
            return PATH_SEP.join(self.path)


class File(Base):
    __tablename__ = 'files'
    file_id = Column(Integer, autoincrement=True, primary_key=True)
    file_name = Column(String(270), nullable=False)
    computer_id = Column(String(20), nullable=False)
    folder_id = Column(Integer, nullable=True)
    md5_hash = Column(String(32), nullable=True)
    last_modified = Column(DateTime)
    last_checked = Column(DateTime)
    folder = None

    def __init__(self, computer_id, folder_id, file_name):
        self.computer_id = computer_id
        self.file_name = file_name
        self.folder_id = self.folder_id
        full_path = self.get_full_path()
        self.md5_hash = md5(full_path)
        self.last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(full_path))
        self.last_checked = datetime.datetime.now()

    def get_full_path(self):
        global PATH_SEP
        return PATH_SEP.join(s.query(Folder).get(self.folder_id), self.file_name)

    def check_exists(self):
        self.last_checked = datetime.datetime.now()
        return os.path.exists(self.get_full_path())


def find_directory(directory):
    if not os.path.isdir(directory) or len(directory):
        return None
    if len(directory) < 270:
        return s.query(Folder).filter(Folder)
    else:
        path_parts = directory.split(PATH_SEP)


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
        if os.path.isdir(entry):
            folder = Folder(computer_id, root_folder.folder_id, entry)
            matching_folder = s.query(Folder).filter(Folder.path == folder.path).first()
            if matching_folder:
                folder = matching_folder
            else:
                s.add(folder)
                s.commit()

            populate_db(computer_id, folder)

        elif os.path.isfile(entry):
            file2 = File(computer_id, root_folder.folder_id, entry)
            matching_file = s.query(File)\
                .filter(File.folder_id == file1.folder_id, File.file_name == file1.file_name).first()
            if matching_file:
                file2 = matching_file
            else:
                s.add(file2)
                s.commit()


def remove_copies_in_same_directory(path):

    print 'to remove same dir:'
    from os.path import join
    for root, dirs, files in os.walk(path):
        for f in files:
            file1 = File(get_computer_id(), root, f)
            existing_copy = s.query(File)\
                .filter(File.md5_hash == file1.md5_hash)\
                .filter(File.folder_id == file1.folder_id)\
                .filter(File.file_name != file1.file_name)\
                .first()
            if existing_copy:
                print file1.FullPath, existing_copy.FullPath,
                print file1.md5_hash, existing_copy.MD5Hash


def prune(directory):
### does not identify copies in same dir!!

    print 'to prune:'
    from os.path import join
    for root, dirs, files in os.walk(directory):
        for f in files:

            file = File(get_computer_id(), root, f)
            existingcopy = s.query(File).filter(File.md5_hash== file.md5_hash).filter(File.FullPath!=file.FullPath).first()
            if  existingcopy and (directory not in existingcopy.FullPath):
                print file.FullPath, existingcopy.FullPath,
                print  file.md5_hash, existingcopy.MD5Hash
                #os.remove(file.FullPath)

def main(argv):

    try:
        #populate_db(directory1)
        #populate_db(directory2)
        #populate_db(directory3)
        pass
    except (Exception):
        pass
    remove_copies_in_same_directory(directory3)
    prune(directory3)




Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
s = Session()


if __name__ == "__main__":
    main(sys.argv[1:])

