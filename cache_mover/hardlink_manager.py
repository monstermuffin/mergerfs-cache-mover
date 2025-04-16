import os
import ctypes
import errno
import glob
import logging
import stat
from pathlib import Path

_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_lgetxattr = _libc.lgetxattr
_lgetxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]

def lgetxattr(path, name):
    if type(path) == str:
        path = path.encode(errors='backslashreplace')
    if type(name) == str:
        name = name.encode(errors='backslashreplace')
    length = 64
    while True:
        buf = ctypes.create_string_buffer(length)
        res = _lgetxattr(path, name, buf, ctypes.c_size_t(length))
        if res >= 0:
            return buf.raw[0:res]
        else:
            err = ctypes.get_errno()
            if err == errno.ERANGE:
                length *= 2
            elif err == errno.ENODATA:
                return None
            else:
                logging.debug(f"Error getting xattr {name} for {path}: {os.strerror(err)}")
                return None

def xattr_basepath(fullpath):
    basepath = lgetxattr(fullpath, 'user.mergerfs.basepath')
    if basepath is not None:
        return basepath.decode(errors='backslashreplace')
    return None

def xattr_relpath(fullpath):
    relpath = lgetxattr(fullpath, 'user.mergerfs.relpath')
    if relpath is not None:
        return relpath.decode(errors='backslashreplace')
    return None

def mergerfs_srcmounts(ctrlfile):
    srcmounts_raw = lgetxattr(ctrlfile, 'user.mergerfs.srcmounts')
    if srcmounts_raw is not None:
        srcmounts = srcmounts_raw.decode(errors='backslashreplace').split(':')
        expanded_srcmounts = []
        for srcmount in srcmounts:
            expanded_srcmounts.extend(glob.glob(srcmount))
        return expanded_srcmounts
    return []

def mergerfs_control_file(path):
    path = os.path.abspath(path)
    while path != os.path.sep:
        ctrlfile = os.path.join(path, '.mergerfs')
        if os.path.exists(ctrlfile):
            return ctrlfile
        path = os.path.abspath(os.path.join(path, os.pardir))
    return None

def find_physical_path(file_path):
    if not os.path.exists(file_path):
        logging.error(f"File does not exist: {file_path}")
        return []
    
    relpath = xattr_relpath(file_path)
    if not relpath:
        logging.debug(f"Could not get relative path for {file_path}")
        return []
    
    ctrlfile = mergerfs_control_file(file_path)
    if not ctrlfile:
        logging.debug(f"Could not find .mergerfs control file for {file_path}")
        return []
    
    srcmounts = mergerfs_srcmounts(ctrlfile)
    if not srcmounts:
        logging.debug(f"Could not get source mounts for {file_path}")
        return []
    
    found_paths = []
    for srcmount in srcmounts:
        potential_path = os.path.join(srcmount, relpath.lstrip('/'))
        if os.path.exists(potential_path):
            found_paths.append(potential_path)
    
    if not found_paths:
        logging.debug(f"Could not find physical path for {file_path}")
    
    return found_paths

def create_hardlink_safe(source, target, backing_path):
    try:
        os.link(source, target)
        return True
    except OSError as e:
        if e.errno == errno.EXDEV:
            logging.warning(f"Cross-device link error, attempting direct disk access: {source} -> {target}")
            return create_hardlink_on_same_disk(source, target, backing_path)
        else:
            logging.error(f"Failed to create hardlink: {e}")
            return False

def create_hardlink_on_same_disk(source, target, backing_path):
    logging.debug(f"Starting create_hardlink_on_same_disk: source={source}, target={target}")
    
    logging.debug("Finding physical path of source file")
    source_physical_paths = find_physical_path(source)
    if not source_physical_paths:
        logging.error(f"Could not find physical path for source: {source}")
        return False
    
    logging.debug(f"Found physical paths: {source_physical_paths}")
    
    logging.debug("Getting relative path of source file")
    source_relpath = xattr_relpath(source)
    if not source_relpath:
        logging.error(f"Could not get relative path for source: {source}")
        return False
    
    logging.debug(f"Source relative path: {source_relpath}")
    
    target_relpath = os.path.relpath(target, backing_path)
    logging.debug(f"Target relative path: {target_relpath}")
    
    for source_physical in source_physical_paths:
        logging.debug(f"Processing physical path: {source_physical}")
        
        source_base = os.path.dirname(source_physical)
        logging.debug(f"Initial source_base: {source_base}")
        
        logging.debug(f"source_relpath={source_relpath}, source_base={source_base}")
        try:
            parent_dir = os.path.dirname(source_relpath) if source_relpath else ""
            logging.debug(f"parent_dir={parent_dir}")
            
            if parent_dir and source_base.endswith(parent_dir):
                source_base = source_base[:-len(parent_dir)]
                logging.debug(f"Adjusted source_base: {source_base}")
        except Exception as e:
            logging.error(f"Exception during base path calculation: {e}")
            continue
        
        physical_target = os.path.join(source_base, target_relpath)
        physical_target_dir = os.path.dirname(physical_target)
        
        logging.debug(f"physical_target={physical_target}, physical_target_dir={physical_target_dir}")
        
        try:
            if not os.path.exists(physical_target_dir):
                logging.debug(f"Creating directory: {physical_target_dir}")
                os.makedirs(physical_target_dir, exist_ok=True)
                logging.info(f"Created directory: {physical_target_dir}")
            
            logging.debug(f"Creating hardlink: {source_physical} -> {physical_target}")
            os.link(source_physical, physical_target)
            logging.info(f"Created direct hardlink: {source_physical} -> {physical_target}")
            return True
        except OSError as e:
            logging.warning(f"Failed to create hardlink on disk {source_base}: {e}")
    
    logging.error(f"Failed to create hardlink for {source} -> {target} on any physical disk")
    return False 