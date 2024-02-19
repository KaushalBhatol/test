# -env , -start_year -end_year -sleep_time
# empty print statement to give one line empty space at start
print()
#

import common_lib
import argparse

# objects
main_log = common_lib.Logger("MAIN")

# logic Handler
import logic_handler
handle = logic_handler.logic()


main_log.info("START: MAIN PROGRAM")

## Argument Parsing
parser = argparse.ArgumentParser()
parser.add_argument('-env', choices=["QA", "PROD", "qa", "prod"], default="QA",help="Give any from QA or PROD")
parser.add_argument('-start_year',type=int, default=2000, help="Select Start Year for Random year selection.")
parser.add_argument('-end_year', type=int, default=3000, help="Select Start Year for Random year selection.")
parser.add_argument('-sleep_time', type=int,default=2, help="Sleept time(seconds) between record insertation i")
args = parser.parse_args()


# handling provided data
try:
    handle.validate_inputs(args)
    main_log.info("ARGUMENTS VALIDATION SUCCESS")
except Exception as e:
    print("Please enter valid arguments!!")
    main_log.critical("ARGUMENT VALIDATION FAIELD")
    main_log.error(f"Error: {e}")
    main_log.warning("CLOSING THE PROGRAM")
    main_log.info("END: MAIN PROGRAM")
    exit()


## STEP 2 SETTING UP CONFIGRATION FILE ACCORDING USER CHOICE
main_log.info("STEP 2: SETTING UP CONFIGRATION FILE ACCORDING USER CHOICE")

if str(args.env).lower() == "qa":
    main_log.debug("APPLAYING QA CONFIGRATION")
    handle.setConfigMode("QA")

else:
    main_log.debug("APPLAYING PROD CONFIGRATION")
    handle.setConfigMode("PROD")

## STEP 3 SETTING UP DATABASE
main_log.debug("STEP 3: SETTING UP DATABASE")
try:
    handle.connect_db()
except Exception as e:
    print("FAIELD TO CONNECT DATABASE")
    exit()

## STEP 4: INGESTION
main_log.debug("STEP 4: INGESTION")
try:
    handle.ingestion()
except Exception as e:
    print("INTERNAL FAILUER")
    exit()



## FOOTER SECTION

# CLOSING DATABASE CONNECTIONS
main_log.info("CLOSING DATABASE CONNECTIONS")
handle.close_db()

main_log.info("END: MAIN PROGRAM")
print() # Blank print statement to give one line space at the end.
exit()