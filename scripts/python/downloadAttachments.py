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


def create_filename(bad_filename):
    # Create filename
    if os.name == 'nt':
        # on windows, this is harder 
        # see https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename

        bad_chars= re.compile(r'[^A-Za-z0-9_. ]+|^\.|\.$|^ | $|^$')
        bad_names= re.compile(r'(aux|com[1-9]|con|lpt[1-9]|prn)(\.|$)')
        clean_title = bad_chars.sub('_', bad_filename)
        if bad_names.match(clean_title) :
            clean_title = '_'+clean_title

    else :

        bad_chars = [';', ':', '!', "*", '/', '\\']
        clean_title = filter(lambda i: i not in bad_chars, bad_filename)
        clean_title = ''.join(list(clean_title))

    return clean_title


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

    parent_recordid, attachment_recordid, filename, config, sf = args

    output_directory = config['salesforce']['output_dir']
    structured = config['salesforce']['structured']

    if structured == 'True':
        # create folder for each parent record
        filename = attachment_recordid + '_' + create_filename(filename)
        output_directory = output_directory + parent_recordid + '/'
        if not os.path.isdir(output_directory):
            os.mkdir(output_directory)
    else:
        filename = parent_recordid + '_' + attachment_recordid + '_' + create_filename(filename)



    url = "https://%s/services/data/v61.0/sobjects/Attachment/%s/Body" % (sf.sf_instance, attachment_recordid)

    logging.debug("Downloading from " + url)
    response = requests.get(url, headers={"Authorization": "OAuth " + sf.session_id,
                                          "Content-Type": "application/octet-stream"})
    if response.ok:
        # Save File
        with open(output_directory + filename, "wb") as output_file:
            output_file.write(response.content)

        return "Saved file to %s" % filename
    else:
        return "Couldn't download %s" % url


def fetch_attachments(sf, config, content_document_links=None, output_directory=None, filename_csv=None,
                filename_pattern=None, content_document_id_name='ContentDocumentId', batch_size=100):
    # the goal of this funtion is to only download all of the attachment record ids (and other fields)
    # we are expecting millions of records so we use the bulk api to get them and save them so we 
    # don't need to do this again.
    objectname = config['restrictions']['objectname']
    objectwhere = config['restrictions']['objectwhere']
    startdate = config['restrictions']['startdate']
    enddate = config['restrictions']['enddate']
    resume = config['salesforce']['resume']

    if resume == 'True':
        return
    
    # if we have an objectname we need to query to find the KeyPrefix for that object name.
    # we will use that prefix to query the attachment object
    if objectname:
        object_prefix = sf.query("SELECT KeyPrefix FROM EntityDefinition WHERE QualifiedApiName = '%s'" % objectname)
        key_prefix = object_prefix['records'][0]['KeyPrefix']

    with open(filename_csv, 'w', encoding='UTF-8', newline='') as results_csv:
        filewriter = csv.writer(results_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        filewriter.writerow(['Id', 'ParentId', 'Name', 'IsPrivate', 'ContentType', 'BodyLength', 'OwnerId', 'CreatedDate', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'SystemModstamp', 'Description', 'IsPartnerShared'])


    query_string = "SELECT Id, ParentId, Name, IsPrivate, ContentType, BodyLength, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, Description, IsPartnerShared FROM Attachment "
    
    if objectname:
        query_string = query_string + " WHERE ParentId > '" + key_prefix + "000000000000000' and ParentId < '" + key_prefix + "999999999999999'"
        if objectwhere:
            query_string = query_string + " AND " + objectwhere
    else:
        if startdate and enddate:
            query_string = query_string + " WHERE CreatedDate >= " + startdate + " AND CreatedDate <= " + enddate

    query_string = query_string + " ORDER BY CreatedDate "

    bulk_results = sf.bulk2.Account.query(query_string, max_records=10000)
    for i, data in enumerate(bulk_results):
        print(data)
        csv_writer_lock.acquire()
        with open(filename_csv, 'a', encoding='UTF-8', newline='') as results_csv:
            # Skip the first line of the data
            results_csv.write('\n'.join(data.split('\n')[1:]))
        csv_writer_lock.release()

def process_records_in_csv(sf, config):

    # standard path to an attachment /services/data/v61.0/sobjects/Attachment/{recordid}/Body
    filename_csv = config['salesforce']['filename_csv']
    resume = config['salesforce']['resume']
    resumeAtId = config['salesforce']['resumeAtId']
    resumeAtIdFound = False

    # read the csv file and call download_files for each record in a thread safe way
    with open(filename_csv, 'r', encoding='UTF-8', newline='') as results_csv:
        file_reader = csv.reader(results_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        next(file_reader) # skip the header row
        for row in file_reader:
            attachment_recordid = row[0]
            parent_recordid = row[1]
            filename = row[2]
            logging.debug(attachment_recordid)
            if resume == 'True' and resumeAtIdFound == False:
                if attachment_recordid == resumeAtId:
                    logging.debug("Found %s" % resumeAtId)
                    resumeAtIdFound = True
                else:
                    logging.debug("Skipping %s" % attachment_recordid)
                    continue
            
            download_file((parent_recordid, attachment_recordid, filename, config, sf))
            logging.debug("Downloaded file %s" % filename)



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
    config = configparser.RawConfigParser(allow_no_value=True)
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

#    content_document_links = sf.query_all(content_document_query)["records"]
#    logging.info("Found {0} total files".format(len(content_document_links)))

    # Begin Downloads
    global_lock = threading.Lock()
    fetch_attachments(sf=sf, config=config, filename_csv=filename_csv, output_directory=output_directory)
    process_records_in_csv( sf=sf, config=config)

if __name__ == "__main__":
    main()
