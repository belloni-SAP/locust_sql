# locust_sql

### Run the test
Run the script 
`prepare_test.sh`
This script download in `/tmp` the needed data to run the test.

Run locust:
* run locust as local runner:
```locust -f postgres_test.py```

* run 2 instances of locust
    * master:
       ```locust -f postgres_test.py --master``` 
    * worker:
       ```locust -f postgres_test.py --worker``` 

Start a test via the UI.

### Note:
To start the scripts it is needed
* docker (the user started the script should have `sudo` rights)
* python packages: 
    * `locust`
    * `psycopg2` 
    
postgres fixed with psycogreen patch (https://github.com/psycopg/psycogreen/, https://github.com/locustio/locust/issues/1588)
