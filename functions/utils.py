import hashlib


def encriptLanguage(language):
    # Encript the value of the parameter
    try:
        return hashlib.sha1(str(language).encode('utf-8')).hexdigest()
    except Exception as e:
        return e
def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData