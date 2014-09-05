Clinical Trials Python Modules
==============================

A set of classes to be used in projects related to data from ClinicalTrials.gov.
There is an effort to bring these to Python 3, however the migration of the NLP-related classes is not yet done.

The non-NLP related classes have been extracted into [py-clinical-trials][pyct].

#### Abandoned Classes ####

Currently unused classes:

```
analyzable.py
eligibilitycriteria.py
```


Trial Data
----------

There is a `Trial` superclass intended to represent a ClinicalTrials.gov trial.
It is designed to work off of JSON data.

The `TrialServer` class is intended to be subclassed and can be used to retrieve _Trial_ instances from a specific server.
A subclass connecting to [LillyCOI's v2][lillycoi] trial API server is included.
That class also contains a _Trial_ subclass `LillyTrial` to facilitate working with extra data provided by Lilly.

[pyct]: https://github.com/p2/py-clinical-trials
[lillycoi]: https://developer.lillycoi.com


NLP
---

Some classes deal with natural language processing and are at an experimental stage at best.

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
