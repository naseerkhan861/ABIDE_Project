# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 16:24:46 2018

@author: Atif. 2018
Code modified from 
Author: Daniel Clark, 2015

"""
def collect_and_download(derivative, pipeline, strategy, out_dir):


    '''
    
    Function to collect and download images from the ABIDE preprocessed
    directory on FCP-INDI's S3 bucket

    Parameters
    ----------
    derivative : string
        derivative or measure of interest
    pipeline : string
        pipeline used to process data of interest
    strategy : string
        noise removal strategy used to process data of interest
    out_dir : string
        filepath to a local directory to save files to
    

    Returns
    -------
    None
        this function does not return a value; it downloads data from
        S3 to a local directory
        
        
    Sample call 
    collect_and_download("rois_all", "dparsf","filt_global" ,"data",);
    downloads ALL ROIs, pipeline is DPARSF with filt_global strategy and store in the "data" directory    
    '''

    derivative = "rois_aal"
    pipeline= "dparsf"
    strategy= "filt_global"
    out_dir = "data"
    # Import packages
    import os
    import urllib
    #following is my change as new version
    from urllib import  request

    # Init variables
    mean_fd_thresh = 0.2
    s3_prefix = 'https://s3.amazonaws.com/fcp-indi/data/Projects/'\
                'ABIDE_Initiative'
    s3_pheno_path = '/'.join([s3_prefix, 'Phenotypic_V1_0b_preprocessed1.csv'])

    # Format input arguments to be lower case, if not already
    derivative = derivative.lower()
    pipeline = pipeline.lower()
    strategy = strategy.lower()

    # Check derivative for extension
    if 'roi' in derivative:
        extension = '.1D'
    else:
        extension = '.nii.gz'

    # If output path doesn't exist, create it
    if not os.path.exists(out_dir):
        print('Could not find %s, creating now...' % out_dir)
        os.makedirs(out_dir)

    # Load the phenotype file from S3
    #s3_pheno_file = urllib.urlopen(s3_pheno_path)
    s3_pheno_file = urllib.request.urlopen(s3_pheno_path)
    pheno_list = s3_pheno_file.readlines()
    pheno_list=[a.decode('ascii') for a in pheno_list]


    print("pheno_list",pheno_list)

    # Get header indices
    ########################################################################################
    header = pheno_list[0].split(',')
    try:
        site_idx = header.index('SITE_ID')
        file_idx = header.index('FILE_ID')
        age_idx = header.index('AGE_AT_SCAN')
        sex_idx = header.index('SEX')
        mean_fd_idx = header.index('func_mean_fd')
    except Exception as exc:
        err_msg = 'Unable to extract header information from the pheno file: %s'\
                  '\nHeader should have pheno info: %s\nError: %s'\
                  % (s3_pheno_path, str(header), exc)
        raise Exception(err_msg)

    # Go through pheno file and build download paths
    print('Collecting images of interest...')
    s3_paths = []
    for pheno_row in pheno_list[1:]:

        # Comma separate the row
        cs_row = pheno_row.split(',')

        try:
            # See if it was preprocessed
            row_file_id = cs_row[file_idx]
            # Read in participant info
            row_site = cs_row[site_idx]
            row_age = float(cs_row[age_idx])
            row_sex = cs_row[sex_idx]
            row_mean_fd = float(cs_row[mean_fd_idx])
        except Exception as exc:
            err_msg = 'Error extracting info from phenotypic file, skipping...'
            print(err_msg)
            continue

        # If the filename isn't specified, skip
        if row_file_id == 'no_filename':
            continue
        # If mean fd is too large, skip
        if row_mean_fd >= mean_fd_thresh:
            continue

        filename = row_file_id + '_' + derivative + extension
        s3_path = '/'.join([s3_prefix, 'Outputs', pipeline, strategy,
                                   derivative, filename])
        print('Adding %s to download queue...' % s3_path)
        s3_paths.append(s3_path)
     
    # s3_paths contains all paths to be downloaded     
        
        
    # And download the items
    total_num_files = len(s3_paths)
    print('\n\n*****************************\n total {} files to download \n*******************************\n\n'.format(total_num_files))
    for path_idx, s3_path in enumerate(s3_paths):
        rel_path = s3_path.lstrip(s3_prefix)
        rel_path = rel_path.replace("/","\\")
        download_file = os.path.join(out_dir, rel_path)
        download_dir = os.path.dirname(download_file)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
#        try:
        if not os.path.exists(download_file):
            print('Retrieving: %s' % download_file)
            #urllib.urlretrieve(s3_path, download_file)
            testfile = urllib.request.URLopener()
            testfile.retrieve(s3_path, download_file)
            print('%.1f%% percent complete' % \
                  (100*(float(path_idx+1)/total_num_files)))
        else:
            print('File %s already exists, skipping...' % download_file)
#        except Exception as exc:
#            print('There was a problem downloading %s.\n'\
#                  'Check input arguments and try again.' % s3_path)

    # Print all done
    print('Done!')

if __name__ == '__main__':
    collect_and_download("rois_all", "dparsf","filt_global" ,"data")
