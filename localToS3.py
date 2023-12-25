import boto3, os, logging, re, csv
from botocore.exceptions import ClientError
from pypdf import PdfReader

#AWS PARAMETERS
REGION = ''
BUCKET_NAME = ""
ACCESS_KEY_ID = ""
SECRET_ACCESS_KEY = ""

UUID_FILE = "" #csv file with uuid values
DIRECTORY = "" #directory to licenses folder
PATH_WRITE_FILE = '' #output file with paths to user folders in aws



logging.basicConfig(level = logging.DEBUG, filename = "log.log", filemode = "w",
                    format = "%(asctime)s - %(levelname)s - %(lineno)d - %(message)s",
                    datefmt="%d-%b-%y %H:%M:%S")
uuid_dict = dict()
write_dict = dict()


def upload_file(file_name, key, bucket):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param key: Folder and file name of file in aws
    :param bucket: Bucket to upload to
    """
    
    s3_resource = boto3.resource(
        service_name = 's3', 
        region_name = REGION, 
        aws_access_key_id = ACCESS_KEY_ID, 
        aws_secret_access_key = SECRET_ACCESS_KEY)
    
    try:
        s3_resource.Bucket(bucket).upload_file(Filename = file_name, Key = key)
    except ClientError as e:
        logging.error(e)
        return False



def main(dir,fol):
    """
    param dir: current directory to iterate through
    param fol: folder the directory is in
    """
    logging.debug(f"Iterating through {dir}")
    for filename in os.listdir(dir):
        f = os.path.join(dir, filename)
        if os.path.isdir(f):
            main(f,uuid_dict[filename]) #have to change this to actually create a folder\
        elif os.path.isfile(f):
            file_path = fol + '/licenses/' + filename
            logging.debug(f"Uploading file {f} with path of {file_path}")
            upload_file(f, file_path, BUCKET_NAME)
            write_dict[fol] = "s3://" + BUCKET_NAME + "/" + fol

def read_file(file_name):
    """
    param file_name: file name of where to read from
    """
    logging.debug(f"Reading from {file_name}")
    with open(file_name, 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader: 
            if row[0] != 'user_id':
                uuid_dict[row[0]] = row[1]

def write_file(file_name):
    """
    param file_nam: file name of where to write to
    """
    logging.debug(f"Writing to {file_name}")
    with open(file_name, 'w',newline ='') as file: 
        writer = csv.writer(file)
        writer.writerow(["UUID","PATH"])
        writer.writerows(write_dict.items())

if __name__ == '__main__':
    logging.debug("Start of the Script")
    try: 
        read_file(UUID_FILE)
        print(uuid_dict)
    except:
        logging.error("Reading UUID CSV File Error", exc_info=True)
        quit()
    
    try: 
        main(DIRECTORY,"")
    except:
        logging.error("Migrating to S3 Error", exc_info=True)
        quit()
    print(write_dict)
    
    
    try:
        write_file(PATH_WRITE_FILE)
    except:
        logging.error("Writing File Paths CSV Error", exc_info=True)
        quit()