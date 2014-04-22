#wopr-etl

ETL code for WOPR.  The structure of the repo is:
* etl_builder
* etl_maps
* meta
* sql
  * init
  * update

The scripts in **etl_builder** are used to generate the metadata and create the init sql scripts for the City of Chicago datasets. **generate_meta.py** extracts the metadata of all datasets (stored under **meta**). **generate_map.py** produces a schema for each dataset (stored under **etl_maps**). **generate_init.py** creates the init sql scripts (stored under **sql/init**) from the dataset schemas.
  
## Running locally

``` bash
git clone git@github.com:svetlozarn/wopr-etl.git
cd wopr-etl/etl_builder

#generate metadata
python generate_meta.py

#generate dataset schemas; must be run before generate_init.py
python generate_map.py

#generate sql init scripts
python generate_init.py
```

## Team

* Svetlozar Nestorov
* Amanda Lund

## Errors / Bugs

If something is not behaving intuitively, it is a bug, and should be reported.
Report it here: https://github.com/svetlozarn/wopr-etl/issues

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Commit, do not mess with rakefile, version, or history.
* Send me a pull request. Bonus points for topic branches.
