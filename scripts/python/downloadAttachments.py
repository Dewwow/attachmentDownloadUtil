import concurrent.futures
from simple_salesforce import Salesforce
import requests
import os
import csv
import re
import logging
import threading

# a global lock used by the batch file downloader to write
# entries to the csv as they're downloaded 
csv_writer_lock = threading.Lock()


def split_into_batches(items, batch_size):
    full_list = list(items)
    for i in range(0, len(full_list), batch_size):
        yield full_list[i:i + batch_size]


def create_filename(title, file_extension, content_document_id, output_directory, filename_pattern):
    # Create filename
    if os.name == 'nt':
        # on windows, this is harder 
        # see https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename

        bad_chars= re.compile(r'[^A-Za-z0-9_. ]+|^\.|\.$|^ | $|^$')
        bad_names= re.compile(r'(aux|com[1-9]|con|lpt[1-9]|prn)(\.|$)')
        clean_title = bad_chars.sub('_', title)
        if bad_names.match(clean_title) :
            clean_title = '_'+clean_title

    else :

        bad_chars = [';', ':', '!', "*", '/', '\\']
        clean_title = filter(lambda i: i not in bad_chars, title)
        clean_title = ''.join(list(clean_title))

    filename = filename_pattern.format(output_directory, content_document_id, clean_title, file_extension)
    return filename


def get_content_document_ids(sf, output_directory, query):

    results_path = output_directory + 'files.csv'
    content_document_ids = set()
    content_documents = sf.query_all(query)

    for content_document in content_documents["records"]:
        content_document_ids.add(content_document["ContentDocumentId"])
        filename = create_filename(content_document["ContentDocument"]["Title"],
                                    content_document["ContentDocument"]["FileExtension"],
                                    content_document["ContentDocumentId"],
                                    output_directory)

    return content_document_ids


def download_file(args):

    record, output_directory, sf, results_path = args

    url = "https://%s%s" % (sf.sf_instance, record["Body"])

    logging.debug("Downloading from " + url)
    response = requests.get(url, headers={"Authorization": "OAuth " + sf.session_id,
                                          "Content-Type": "application/octet-stream"})
    if response.ok:
        # Save File
        filename = record["Name"]
        with open(output_directory + filename, "wb") as output_file:
            output_file.write(response.content)

            # write file entry to csv
            csv_writer_lock.acquire()
            with open(results_path, 'a', encoding='UTF-8', newline='') as results_csv:
                filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                filewriter.writerow([
                    record["CreatedDate"],
                    record["ParentId"],
                    record["Id"],
                    record["IsDeleted"],
                    record["Name"],
                    record["IsPrivate"],
                    record["ContentType"],
                    record["BodyLength"],
                    record["Body"],
                    record["OwnerId"],
                    record["CreatedById"],
                    record["LastModifiedDate"],
                    record["LastModifiedById"],
                    record["SystemModstamp"],
                    record["Description"],
                    record["IsPartnerShared"]
                ])
            csv_writer_lock.release()

        return "Saved file to %s" % filename
    else:
        return "Couldn't download %s" % url


def fetch_attachments(sf, content_document_links=None, output_directory=None, filename_csv=None,
                filename_pattern=None, content_document_id_name='ContentDocumentId', batch_size=100):

    query_string = "SELECT Id, IsDeleted, ParentId, Name, IsPrivate, ContentType, BodyLength, Body, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, Description, IsPartnerShared FROM Attachment ORDER BY CreatedDate "
    query_string = "SELECT Id, Name FROM Account ORDER BY CreatedDate "
    bulk_results = sf.bulk2.Account.query(query_string, max_records=1)
    for i, data in enumerate(bulk_results):
        print(data)
        csv_writer_lock.acquire()
        with open(filename_csv, 'a', encoding='UTF-8', newline='') as results_csv:
            # Skip the first line of the data
            results_csv.write('\n'.join(data.split('\n')[1:]))
        csv_writer_lock.release()
     

 #   while query_response:
 #       with concurrent.futures.ProcessPoolExecutor() as executor:
 #           args = ((record, output_directory, sf, results_path)
 #                   for record in query_response["records"])
#
#            for esult in executor.map(download_file, args):
#                logging.debug(result)
#        break
        
    logging.debug('All records.')


def main():
    import argparse
    import configparser
    import threading

    # Process command line arguments
    parser = argparse.ArgumentParser(description='Export ContentVersion (Files) from Salesforce')
    parser.add_argument('-f', '--filenamepattern', metavar='filenamepattern', required=False, default='{0}{1}-{2}.{3}',
                        help='Specify the filename pattern for the output, available values are:'
                             '{0} = output_directory, {1} = content_document_id, {2} title, {3} file_extension, '
                             'Default value is: {0}{1}-{2}.{3} which will be '
                             '/path/ContentDocumentId-Title.fileExtension')
    args = parser.parse_args()

    # Get settings from config file
    config = configparser.ConfigParser(allow_no_value=True)
    config.read('downloadAttachments.ini')

    username = config['salesforce']['username']
    password = config['salesforce']['password']
    token = config['salesforce']['security_token']

    filename_csv = config['salesforce']['filename_csv']

    is_sandbox = config['salesforce']['connect_to_sandbox']
    if is_sandbox == 'True':
        domain = 'test'

    # custom domain overrides "test" in case of sandbox
    domain = config['salesforce']['domain']
    if domain:
        domain += '.my'
    else:
        domain = 'login'

    output_directory = config['salesforce']['output_dir']
    batch_size = int(config['salesforce']['batch_size'])
    loglevel = logging.getLevelName(config['salesforce']['loglevel'])

    # Setup logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=loglevel)

    logging.info('Export ContentVersion (Files) from Salesforce')
    logging.info('Username: ' + username)
    logging.info('Signing in at: https://'+ domain + '.salesforce.com')

    # Connect to Salesforce
    sf = Salesforce(username=username, password=password, security_token=token, domain=domain)
    logging.debug("Connected successfully to {0}".format(sf.sf_instance))

    # initialize the csv file header row
    logging.info('Output directory: ' + output_directory)
    if not os.path.isdir(output_directory):
        os.mkdir(output_directory)
 
    with open(filename_csv, 'w', encoding='UTF-8', newline='') as results_csv:
        filewriter = csv.writer(results_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['CreatedDate', 'ParentId', 'Id', 'IsDeleted',  'Name', 'IsPrivate', 'ContentType', 'BodyLength', 'Body', 'OwnerId', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'SystemModstamp', 'Description', 'IsPartnerShared'])



#    content_document_links = sf.query_all(content_document_query)["records"]
#    logging.info("Found {0} total files".format(len(content_document_links)))

    # Begin Downloads
    global_lock = threading.Lock()
    fetch_attachments(sf=sf, filename_csv=filename_csv, output_directory=output_directory)

if __name__ == "__main__":
    main()