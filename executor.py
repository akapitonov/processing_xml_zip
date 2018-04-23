import csv
import pathlib
import uuid
import xml.etree.ElementTree as ET
import zipfile
from functools import partial
from multiprocessing import Pool, Manager
from random import randint

ARCHIVES_DIR_NAME = 'archives'
FILENAME_CSV_LEVELS = 'levels.csv'
FILENAME_CSV_OBJECTS = 'objects.csv'
COUNT_XML_IN_ARCHIVE = 100


def create_xml_file():
    """Create one xml file in memory and return it."""
    root = ET.Element('root')
    ET.SubElement(root, 'var', attrib={'name': 'id', 'value': str(uuid.uuid4())})
    ET.SubElement(root, 'var', attrib={'name': 'level', 'value': str(randint(1, 99))})
    objects = ET.SubElement(root, 'objects')
    for i in range(randint(1, 10)):
        ET.SubElement(objects, 'object', attrib={'name': str(uuid.uuid4())})
    return ET.tostring(root)


def create_zip(filename):
    """Create one zip archive with xml files."""
    with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as arch:
        for j in range(1, COUNT_XML_IN_ARCHIVE + 1):
            xml_file = create_xml_file()
            xml_filename = 'xml_file_{}.xml'.format(j)
            arch.writestr(xml_filename, xml_file)


def create_path_zips(count=50):
    """Create paths for archives and return them."""
    zips = []
    for i in range(1, count + 1):
        arch_filename = 'archive_{}.zip'.format(i)
        arch_name = pathlib.PurePath(ARCHIVES_DIR_NAME, arch_filename)
        zips.append(arch_name)
    return zips


def process_zip(filename, queue_file_levels, queue_file_objects):
    """Process a zip."""
    with zipfile.ZipFile(filename, 'r') as arch:
        xml_list = arch.namelist()
        for xml in xml_list:
            with arch.open(xml) as xml_file:
                content = xml_file.read()
                tree = ET.fromstring(content)
                doc_id = tree.find(".//var[@name='id']").attrib['value']
                doc_level = tree.find(".//var[@name='level']").attrib['value']
                queue_file_levels.put([doc_id, doc_level])
                objects = [(doc_id, obj.attrib['name']) for obj in tree.findall('.//object')]
                queue_file_objects.put(objects)


def search_zips():
    """Return path's arhives for processing."""
    directory = pathlib.Path(ARCHIVES_DIR_NAME)
    if not directory.exists():
        raise Exception('No directory with data.')
    return [item for item in directory.iterdir() if not item.is_dir() and item.suffix.lower() == '.zip']


def csv_writer_levels(queue):
    """Worker is writing to csv file levels."""
    with open(FILENAME_CSV_LEVELS, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        while True:
            id, level = queue.get()
            if not level:
                break
            csv_writer.writerow([id, level, ])


def csv_writer_objects(queue):
    """Worker is writing to csv file objects."""
    with open(FILENAME_CSV_OBJECTS, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        while True:
            objects = queue.get()
            if not objects:
                break
            csv_writer.writerows(objects)


def start_task_create_zips():
    """Start creating zips task."""
    pathlib.Path(ARCHIVES_DIR_NAME).mkdir(parents=True, exist_ok=True)
    archives = create_path_zips()
    pool = Pool()
    pool.map(create_zip, archives)
    pool.close()
    pool.join()


def start_task_process_zips():
    """Start processing zips task."""
    zips = search_zips()
    manager = Manager()
    queue_file_levels = manager.Queue()
    queue_file_objects = manager.Queue()
    pool = Pool()
    pool.apply_async(csv_writer_levels, args=(queue_file_levels,))
    pool.apply_async(csv_writer_objects, args=(queue_file_objects,))
    pool.map(partial(process_zip, queue_file_levels=queue_file_levels, queue_file_objects=queue_file_objects), zips)
    queue_file_levels.put([None, None])
    queue_file_objects.put([])
    pool.close()
    pool.join()


if __name__ == '__main__':
    print("Available program mode:")
    print("1 - create zips with xml files")
    print("2 - process zips with xml files")
    print("3 - exit")
    while True:
        type_task = input("Enter choice[1-3]:")
        if type_task == '1':
            print('Program is creating archives, please wait...')
            start_task_create_zips()
            break
        elif type_task == '2':
            print('Program is processing archives, please wait...')
            start_task_process_zips()
            break
        elif type_task == '3':
            break
        else:
            print('Please, type correct option, type 3 for exit')
    print('Program was completed.')
