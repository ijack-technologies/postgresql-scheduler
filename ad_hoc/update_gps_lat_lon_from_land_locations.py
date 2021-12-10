#######################################################################################################
# This module is meant to be run from development only, not from the production server/Docker container
# It updates the gps_lat and gps_lon fields in the public.structures table, using the API from legallandconverter.com
# API instructions here: https://legallandconverter.com/p51.html#OVERVIEW
# It costs USD $0.10 per lookup, so don't be wasteful since there are 500+ lookups ($50)
#######################################################################################################

import logging
import os
import pathlib
import sys
import time

import requests
from dotenv import load_dotenv


# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
def insert_path(pythonpath):
    try:
        sys.path.index(pythonpath)
    except ValueError:
        sys.path.insert(0, pythonpath)


# For running in development only
project_folder = pathlib.Path(__file__).absolute().parent.parent
ad_hoc_folder = project_folder.joinpath("ad_hoc")
cron_d_folder = project_folder.joinpath("cron_d")
insert_path(cron_d_folder)  # second in path
insert_path(ad_hoc_folder)  # first in path

from cron_d.utils import Config, configure_logging, error_wrapper, get_conn, run_query

load_dotenv()
LOG_LEVEL = logging.INFO
LOGFILE_NAME = "update_gps_lat_lon_from_land_locations"
c = Config()
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)

# Warning for the user, in case she started this program by accident. This is a chance to cancel.
yes_or_no = input(
    "Are you sure you want to pay legallandconverter.com USD $0.10/lookup to update all the latitudes and longitudes based on the surface location of each structure? \n(y)es or (n)o: "
)
y_or_n_lower = str(yes_or_no).lower()[0]
if y_or_n_lower == "y":
    c.logger.info("Continuing...")
elif y_or_n_lower == "n":
    c.logger.warning("Exiting now!")
    sys.exit()


@error_wrapper()
def main(c):
    """"""

    # exit_if_already_running(c, pathlib.Path(__file__).name)

    # Keep a connection open for efficiency
    conn = get_conn(c, "ijack")

    try:
        # Find structures with bad GPS coordinates
        SQL = """
            select 
                t1.id as structure_id,
                t1.structure,
                t1.surface,
                t1.gps_lat,
                t1.gps_lon,
                t3.customer,
                t4.power_unit
            from public.structures t1
            left join myijack.structure_customer_rel t2
                on t2.structure_id = t1.id
            left join myijack.customers t3
                on t3.id = t2.customer_id
            left join public.power_units t4
                on t4.id = t1.power_unit_id
            where 
                t1.surface is not null
                and t1.surface != ''
                and (
                    t1.gps_lat is null
                    --Moosomin shop = 50.16311264038086
                    or t1.gps_lat = 50.16311264038086
                    or t1.gps_lon is null
                    --Moosomin shop = -101.6754150390625
                    or t1.gps_lon = -101.6754150390625
                )
        """
        _, rows = run_query(c, SQL, db="ijack", fetchall=True, conn=conn)
        # df = pd.DataFrame(rows, columns=columns)

        # # Get the Boto3 AWS IoT client for updating the "thing shadow"
        # client_iot = get_client_iot()

        n_rows = len(rows)
        time_start = time.time()
        for i, dict_ in enumerate(rows):
            # gps_lat = dict_["gps_lat"]
            # gps_lon = dict_["gps_lon"]
            structure_id = dict_["structure_id"]
            structure = dict_["structure"]
            surface = dict_["surface"]
            customer = dict_["customer"]
            power_unit = dict_["power_unit"]

            # Extract components from surface land location
            surface_upper = surface.upper()

            if "VIRDEN WAREHOUSE" in surface_upper or "IJACK YARD" in surface_upper:
                continue

            unit_info = f"{customer} structure '{structure}' (power unit '{power_unit}') with surface '{surface}'"
            c.logger.info(f"{i+1} of {n_rows}: Getting GPS for {unit_info}")

            # Remove anything before the first "/" (e.g. "06-30/08-30-012-26w1")
            split_f_slash = surface_upper.split("/")[-1]

            # Remove anything after the "W1" (e.g. "W1 East Unit")
            split_f_slash_part_0 = split_f_slash.split(" ")[0]

            # Split off the meridian (e.g. W1 into "1")
            split_on_W = split_f_slash_part_0.split("W")
            if len(split_on_W) != 2:
                c.logger.warning(
                    f"{unit_info} can't be split on the W meridian (e.g. W1). Continuing with next..."
                )
                continue
            loc, meridian = split_on_W
            w_meridian = f"W{meridian}"

            # Only keep digits and dashes (we'll deal with this later...)
            # digits = re.sub("[^0-9,-]", "", loc)

            split_dash = loc.split("-")

            # Convert to integers to remove leading zeros
            # nums = [int(x) for x in split_dash]
            nums = []
            for x in split_dash:
                try:
                    int_ = int(x)
                except ValueError:
                    c.logger.warning(f"Can't convert {x} from {split_dash} to integer!")
                    # c.logger.warning(f"Error: {err}")
                    continue
                else:
                    nums.append(int_)

            if len(nums) != 4:
                c.logger.warning(
                    f"{unit_info} can't be split into four parts. Continuing with next..."
                )
                continue

            quarter, section, township, range_ = nums

            # API instructions here:
            # https://legallandconverter.com/p51.html#OVERVIEW

            api_url = "http://legallandconverter.com/cgi-bin/android5c.cgi"
            # Canadian DLS Query = "legal"
            type_ = "legal"

            # http://legallandconverter.com/cgi-bin/android5c.cgi?username=DEVELOPX&password=TEST1234&quarter=SW&section=24&township=12&range=20&meridian=W4&cmd=legal
            payload = dict(
                username=os.getenv("LEGALLANDCONVERTER_USERNAME", None),
                password=os.getenv("LEGALLANDCONVERTER_PASSWORD", None),
                # LSD number 1-16 or quarter of the section
                quarter=quarter,
                # 36 sections in township
                section=section,
                # township e.g. 1-127 going north (e.g. farm is 13 north of US border)
                township=township,
                # range e.g. 1-34 going west in the meridian (e.g. farm is 32 of 34 so almost in W2)
                range=range_,
                # meridian e.g. 1 MB, 2 SK, 3 AB (roughly)
                meridian=w_meridian,
                cmd=type_,
            )

            r = requests.get(api_url, params=payload)
            assert r.status_code == 200, "r.status_code != 200!"

            # c.logger.debug(f"r.url: {r.url}")
            # c.logger.debug(f"r.content: {r.content}")
            # c.logger.debug(f"r.encoding: {r.encoding}")
            c.logger.debug(f"request.text: {r.text}")

            new_latitude = None
            new_longitude = None
            text = r.text
            for line in text.splitlines():
                line_upper = line.upper()

                if "LATITUDE" in line_upper:
                    new_latitude = line_upper.split(": ")[1]
                elif "LONGITUDE" in line_upper:
                    new_longitude = line_upper.split(": ")[1]
                elif "CREDITS: 0" in line_upper:
                    c.logger.critical("No more credits in account!")
                elif "HAVECONVERSION: 0" in line_upper:
                    c.logger.critical(
                        f"Problem with conversion of {unit_info}! Raising exception now!"
                    )
                    # raise Exception
                else:
                    continue

                # Text Returned:
                # Content-type: text/plain

                # STATUSBAR: Version 1.20 Available
                # LATITUDE: 50.008196
                # LONGITUDE: -112.614401
                # UTM: 12N 384323 5540791
                # MGRS: 12UUA8432240790
                # NTS: D-10-A/82-I-2
                # CREDITS: 29
                # LSD:
                # QUARTER: SW
                # QUARTERLSD: SW
                # USAQUARTER:
                # USASECTION:
                # USATOWNSHIP:
                # USANORTHSOUTH:
                # USARANGE:
                # USAEASTWEST:
                # USAMERIDIAN:
                # USASTATE:
                # COUNTRY: Canada
                # HAVECONVERSION: 1

            if new_latitude is not None and new_longitude is not None:
                c.logger.info(
                    f"{i+1} of {n_rows}: Updating {unit_info}:\nnew latitude: {new_latitude}; new longitude: {new_longitude}"
                )

                sql_update = f"""
                    update public.structures
                    set 
                        gps_lat = {new_latitude},
                        gps_lon = {new_longitude}
                    where id = {structure_id}
                """
                run_query(
                    c, sql_update, db="ijack", fetchall=False, conn=conn, commit=True
                )
            else:
                c.logger.warning(
                    f"{i+1} of {n_rows}: Can't update {unit_info}!\nnew latitude: {new_latitude}; new longitude: {new_longitude}\ntext: {text}"
                )

    except Exception:
        c.logger.exception("Error updating GPS!")
    finally:
        c.logger.info("Closing DB connection...")
        conn.close()
        del conn

    time_finish = time.time()
    c.logger.info(
        f"Time to update all GPS coordinates in the public.structures table: {round(time_finish - time_start)} seconds"
    )

    return None


if __name__ == "__main__":
    main(c)
