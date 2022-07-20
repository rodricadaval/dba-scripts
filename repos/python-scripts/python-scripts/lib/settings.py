class Settings:

    DOMAIN = {
        "a": ".ops.acompanydomain.com.a",
        "b": ".ops.acompanydomain.com.b",
        "c": ".ops.acompanydomain.com",
        "c": ".ops.X1X.cl",
        "p": ".ops.X1X.pe",
        "u": ".ops.acompanydomain.com.u",
        "e": ".ops.acompanydomain..com.e",
        "cc": ".ops.acompanydomain.com.c",
        "m": ".ops.acompanydomain.com.m",
        "dev": ".dev.acompanydomain.com",
        "xx1": ".ops.acompanydomain.com",
        "xx1dev": ".ops.acompanydomain.com"
    }

    REGION = {
        "a": "xx-east-1",
        "b": "xx-west-2",
        "c": "xx-west-2",
        "c": "xx-east-1",
        "p": "xx-east-1",
        "u": "xx-east-1",
        "e": "xx-west-2",
        "cc": "xx-west-2",
        "m": "xx-east-1",
        "dev": "xx-east-1",
        "xx1": "xx-west-2",
        "xx1dev": "xx-west-2",
        "tool": "xx-west-2"
    }

    ECS_CLUSTERS = {
        "a": "Microser-Spread",
        "b": "Microser-PC",
        "c": "Microser-C-PC",
        "c": "Microser-c-PC",
        "p": "Microser-p-PC",
        "u": "Microser-u-PC",
        "e": "Microser-e-PC",
        "cc": "Microser-c-PC",
        "m": "Microser-M-PC",
        "dev": "dev-microser-pc"
    }

    AWS_PROFILE = {        
        "a": "xx-xx-east-1",
        "b": "xx-xx-west-2",
        "c": "xx-xx-east-1",
        "cc": "xx-xx-west-2",
        "e": "xx-xx-west-2",
        "u": "xx-xx-east-1",
        "p": "xx-xx-east-1",
        "c": "xx-xx-west-2",
        "m": "xx1-xx-east-1",
        "dev": "xxx-xxx-east-1",
        "tool": "xx-xx-west-2",
        "xx1": "xxxx-xx-west-2",
        "xx1dev": "xxxx-xx-west-2"
    }

    LOGDNA_COUNTRY = {
        'dev': "ddddddddddd",
        'c': "dddddddddd1",
        'a': "dddddddddd2",
        'p': "dddddddddd3",
        'u': "dddddddddd4",
        'c': "dddddddddd5",
        'm': "dddddddddd6",
        'b': "dddddddddd7",
        'e': "dddddddddd8",
        'c': "dddddddddd9"
    }

    DYNAMO_INVENTORY_TABLE = {
        'dev': "anansiblevar_inventory_ms",
        'c': "anansiblevar_inventory_ms_co",
        'a': "anansiblevar_inventory_ms_ar",
        'p': "anansiblevar_inventory_ms_x1",
        'u': "anansiblevar_inventory_ms_uy",
        'c': "anansiblevar_inventory_ms_cl",
        'm': "anansiblevar_inventory_ms_xx1",
        'b': "anansiblevar_inventory_ms_br",
        'e': "anansiblevar_inventory_ms_ec",
        'c': "anansiblevar_inventory_ms_cr",
        'xx1': "anansiblevar_inventory_ms_xx1_2",
        'xx1dev': "anansiblevar_inventory_ms_xx1_2",
        'tool': "anansiblevar_inventory_ms_co"
    }

    DYNAMO_INVENTORY_RDS = {
        'dev': "dba_test_rds_inventory_dev",
        'c': "dba_test_rds_inventory_co",
        'a': "dba_test_rds_inventory_ar",
        'x1': "dba_test_rds_inventory_x1",
        'u': "dba_test_rds_inventory_uy",
        'ccc': "dba_test_rds_inventory_cl",
        'xx1': "dba_test_rds_inventory_xx1",
        'b': "dba_test_rds_inventory_br",
        'e': "dba_test_rds_inventory_ec",
        'cc': "dba_test_rds_inventory_cr",
        'xx1': "dba_test_rds_inventory_xx1",
        'xx1dev': "dba_test_rds_inventory_xx1",
        'tool': "dba_test_rds_inventory_co"
    }

    DYNAMO_INVENTORY_MS_VARS = {
        'dev': "db_test_ms_vars_inventory_dev",
        'c': "db_test_ms_vars_inventory_co",
        'a': "db_test_ms_vars_inventory_ar",
        'x1': "db_test_ms_vars_inventory_x1",
        'u': "db_test_ms_vars_inventory_uy",
        'ccc': "db_test_ms_vars_inventory_cl",
        'xx1': "db_test_ms_vars_inventory_xx1",
        'b': "db_test_ms_vars_inventory_br",
        'e': "db_test_ms_vars_inventory_ec",
        'cc': "db_test_ms_vars_inventory_cr",
        'xx1': "db_test_ms_vars_inventory_xx1",
        'xx1dev': "db_test_ms_vars_inventory_xx1",
        'tool': "db_test_ms_vars_inventory_co"
    }

    FIVETRAN_LBS = {
        "a": [
            "arn:aws:elasticloadbalancing:xx-east-1:999999999999:loadbalancer/net/nlb-x-a-useast1/ddfb7abfaa8fc5a9"
        ],
        "c": [
            "arn:aws:elasticloadbalancing:xx-east-1:999999999999:loadbalancer/net/nlb-x-c-useast1/69f09df7f9028a87"
        ],
        "p": [
            "arn:aws:elasticloadbalancing:xx-east-1:999999999999:loadbalancer/net/nlb-x-p-useast1/c39849f21f627d14"
        ],
        "u": [
            "arn:aws:elasticloadbalancing:xx-east-1:999999999999:loadbalancer/net/nlb-x-useast1-v2/dfa31d347ed3921e"
        ],
        "b": [
            "arn:aws:elasticloadbalancing:xx-west-2:999999999999:loadbalancer/net/nlb-x-br-uswest2/853b7a2501ccc3b5"
        ],
        #"c": ["arn:aws:elasticloadbalancing:xx-west-2:999999999999:loadbalancer/net/nlb-x-xx-xxwest2/3bfe4796c3035568", "arn:aws:elasticloadbalancing:xx-west-2:999999999999:loadbalancer/net/nlb-x-co-2-uswest2/ebadb68e8ea5bb8c"],
        "c": [
            "arn:aws:elasticloadbalancing:xx-west-2:999999999999:loadbalancer/net/nlb-x-xx-xxwest2/3bfe4796c3035568"
        ],
        "cc": [
            "arn:aws:elasticloadbalancing:xx-west-2:999999999999:loadbalancer/net/nlb-x-cr-uswest2/0e221e3e1c005dbe"
        ],
        "e": [
            "arn:aws:elasticloadbalancing:xx-west-2:999999999999:loadbalancer/net/nlb-x-ec-uswest2/9c7fb36e65989c22"
        ],
        "m": [
            "arn:aws:elasticloadbalancing:xx-east-1:009706192331:loadbalancer/net/nlb-x-x1-xxeast1/e83a173a9222b2f1"
        ],
        "xx1": [
            "arn:aws:elasticloadbalancing:xx-west-2:345885438949:loadbalancer/net/nlb-x-X1Xpay-prod/4809dd344c3136e1"
        ],
        "xx1dev": [
            "arn:aws:elasticloadbalancing:xx-west-2:345885438949:loadbalancer/net/nlb-x-X1Xpay-dev-west-2/750284e4b8e2540c"
        ],
    }

    FIVETRAN_PRIVATE_LINKS = {
        "a": "pg-test.privatelink.acompanydomain.com.a",
        "c": "pg-test.privatelink.acompanydomain.com.c",
        "p": "pg-test.privatelink.acompanydomain.com.p",
        "u": "pg-X1Xtenderos.privatelink.acompanydomain.com.u",
        "b": "pg-test.privatelink.acompanydomain.com.b",
        "c": "pg-test.privatelink.acompanydomain.com.c",
        "c": "pg-test.privatelink.acompanydomain.com.c",
        "e": "pg-test.privatelink.acompanydomain..com.e",
        "m": "pg-test.privatelink.acompanydomain.com.m",
        "xx1": "",
        "xx1dev": ""
    }

    FIVETRAN_VPCS = {
        "a": "vpc-xxxx",
        "c": "vpc-xxxx",
        "p": "vpc-xxxx",
        "u": "vpc-xxxx",
        "b": "vpc-xxxx",
        "c": "vpc-xxxx",
        "cc": "vpc-xxxx",
        "e": "vpc-xxxx",
        "m": "vpc-xxxx",
        "xx1": "vpc-xxxx",
        "xx1dev": "vpc-xxxx"
    }

    FIVETRAN_GROUP_ID_BY_WAREHOUSE = {
        "SFDW": "test",
        "SFDW2": "test2",
        "SFDWDEV": "test3",
        "SFOregon": "test4",
    }

    FIVETRAN_WAREHOUSE_BY_GROUP_ID = {
        "test": "SFDW",
        "test2": "SFDW2",
        "test3": "SFDWDEV",
        "test4": "SFOregon",
    }

    FIVETRAN_PL_WAREHOUSE = {
        "a": "SFDW2",
        "c": "SFDW2",
        "p": "SFDW2",
        "u": "SFDW2",
        "b": "SFOregon",
        "c": "SFOregon",
        "cc": "SFOregon",
        "e": "SFOregon",
        "m": "SFDW2",
        "dev": "SFDWDEV",
        "xx1": "SFDWxx1",
        "xx1dev": "SFDWxx1DEV"
    }

    FIVETRAN_PL_GROUP_ID = {
        "a": "test2",
        "c": "test2",
        "p": "test2",
        "u": "test2",
        "b": "test4",
        "c": "test4",
        "cc": "test4",
        "e": "test4",
        "m": "test2",
        "dev": "test3",
        "xx1": "test4",
        "xx1dev": "test4",
    }

    COUNTRIES = ["a", "c", "m", "b", "p", "u", "c", "e", "cc", "dev", "xx1", "xx1dev"]
    
    REGIONS = ["xx-east-1", "xx-west-2"]

    AWS_PROFILES_UNIQUE = ["xx-xx-east-1", "xx-xx-west-2", "xx1-xx-east-1", "xxx-xxx-east-1", "xxxx-xx-west-2"]                
