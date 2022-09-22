import os
import shutil
from zipfile import ZipFile

month_hash = {
    'jan' : '01',
    'feb' : '02',
    'mar' : '03',
    'apr' : '04',
    'may' : '05',
    'jun' : '06',
    'jul' : '07',
    'aug' : '08',
    'sep' : '09',
    'oct' : '10',
    'nov' : '11',
    'dec' : '12'
}
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_full_path(new_file_name_ext, unzip_locationed):
    naming_format = [i.lower() for i in new_file_name_ext.split('.')[0].split("_")]  
    my_sub_dir_year = f'p_year={naming_format[1]}'
    my_sub_dir_month = f'p_month={month_hash[naming_format[0]]}' 
    
    dir_path = os.path.join(unzip_locationed, my_sub_dir_year, my_sub_dir_month)
    
    if os.path.isdir(dir_path):
        pass
    else:
        os.makedirs(dir_path)
    
    new_file_name_full_path = os.path.join(dir_path, new_file_name_ext)
    
    return new_file_name_full_path
    
    
def unzip_data(dataset_path, unzip_locationed):
    dataset_path = os.getcwd()
    unzip_locationed = os.path.join(dataset_path, 'extracted')
    
    if os.path.isdir(unzip_locationed):
        pass
    else:
        os.mkdir(unzip_locationed)
    
    list_of_zip_files = os.listdir(dataset_path)
    
    list_of_zip_files_abspath = [ os.path.abspath(p) for p in list_of_zip_files if p.endswith('zip')]  
    
    for file in list_of_zip_files_abspath:
        with ZipFile(file, 'r') as zipObj:
            # Extract all the contents of zip file in different directory
            zipObj.extractall(unzip_locationed)
            extracted_file_name = zipObj.infolist()[0].filename 
            extracted_file_name_full_path = os.path.join(unzip_locationed, extracted_file_name)
            
            new_file_name_ext = "_".join(os.path.splitext(file)[0].split('_')[-2::]) + ".csv"
            
            new_file_name_full_path = get_full_path(new_file_name_ext, unzip_locationed)
            # new_file_name_full_path = os.path.join(unzip_locationed, new_file_name_ext)
            shutil.move(extracted_file_name_full_path, new_file_name_full_path)
            
            print(f'File {os.path.basename(file)} is unzipped in {new_file_name_full_path} location') 

def main():
    dataset_path = os.getcwd()
    unzip_locationed = os.path.join(dataset_path, 'extracted')
    
    # Unziping the data
    unzip_data(dataset_path, unzip_locationed)
    
    

if __name__ == '__main__':
    main()
    
    