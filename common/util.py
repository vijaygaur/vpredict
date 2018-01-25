import os
import zipfile

def zipdir(path, name):
    ziph = zipfile.ZipFile(name, 'w', zipfile.ZIP_DEFLATED)
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
    ziph.close()


def read_file(filename):
    with open(filename, 'rb') as f:
        data = f.read()
    return data

def write_file(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)


