ClinicalTrials.gov Modules
==========================

A set of classes to be used in projects related to ClinicalTrials.gov.


### cTAKES ###

To use the ctakes classes we need to install cTAKES:

1. Install Maven (This is for OS X, requires [Homebrew][], adapt accordingly)
    
        brew install maven

2. Run the script `ctakes-extras/ctakes-install.sh`, which will:
    - checkout a copy of cTAKES into `./ctakes-svn` (if you haven't already) or update from the SVN repo
    - package cTAKES using Maven
    - move the compiled version to `./ctakes`
    - copy over the extras in `./ctakes-extras`

3. Create a file named `./umls.sh` containing your UMLS username and password:
      
        UMLS_USERNAME='username'
        UMLS_PASSWORD='password'

> This currently does not work correctly.


### MetaMap ###

To use MetaMap, download and install MetaMap:

1. Download [from NLM](http://metamap.nlm.nih.gov/#Downloads)
2. Extract the archive into our root directory and rename it to `metamap`
3. Copy the script `metamap-extras/run.sh` to `metamap/`
4. Run the install script:
    
        ./metamap/bin/install.sh


### NLTK ###

requirements:

- nltk

