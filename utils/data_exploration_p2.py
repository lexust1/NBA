import logging
import sys
from typing import Tuple


import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
# import plotly.io as pio
from psycopg import sql
import seaborn as sns
# pio.renderers.default='jupyterlab'
from ydata_profiling import ProfileReport

# Set pandas
pd.options.display.width = 200

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "{asctime} | {name} | {levelname} | {message}", style="{"
)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_db_info(cur) -> Tuple[dict, pd.DataFrame]:
    """Creates a dict of DFs.

    Creates a dict of DFs, where the table name is the key and
    every column of DF is an information about column in DB (name,
    type, etc).
    """
    # Retrieve the table names.
    cur.execute("""
        SELECT tablename FROM pg_catalog.pg_tables
         WHERE schemaname = 'public'
    """)

    table_names = [table_name[0] for table_name in cur.fetchall()]
    logger.debug(f"Table names: {table_names}")
    # Retrieve the table data and create a dictionary of dataframes.
    db_info = {}
    row_counts = []
    for table_name in table_names:
        query = """
            SELECT ordinal_position, 
                   column_name, 
                   data_type, 
                   is_nullable, 
                   column_default
              FROM information_schema.columns 
             WHERE table_name = %(table_name)s;
        """
        params = {"table_name": table_name}
        cur.execute(query, params)
        table_info = cur.fetchall()
        db_info[table_name] = pd.DataFrame(
            columns=[
                "ID", "Column Name", "Type", "IS_NULLABLE", "DFLT_VALUE "
            ],
            data=table_info
        ).set_index("ID")
        logger.debug(f"DB info: {db_info}")
        # Count a number of rows in every table.
        query = sql.SQL("""
            SELECT COUNT(*)
              FROM {}
        """).format(sql.Identifier(table_name))
        cur.execute(query)
        row_counts.append(cur.fetchone()[0])
    # Create the num_rows dataframe that will be used in plot-libraries.
    num_rows = pd.DataFrame(
        columns=["Table Name", "A num of rows"],
        data=list(zip(table_names, row_counts))
    )#.set_index("Table Name")
    logger.debug(f"Dataframe with the table names and rows:  {num_rows}")
    # The info log message.
    logger.info("The list of dictionaries db_info has been created.")
    return db_info, num_rows


def plot_mpl_bars(num_rows):
    """Creates the bar plot using matplotlib."""
    # Set the figure, bars, ticks.
    plt.figure(figsize=(12, 5))
    bars = plt.bar(num_rows["Table Name"], num_rows["A num of rows"])
    plt.xlabel("Table Names")
    plt.ylabel("A Number of Rows")
    plt.title("A Number of Rows in Each Table")
    plt.xticks(rotation=90)
    # Add grids
    plt.grid(True, which="both", axis="y", linestyle="--", linewidth=0.5)
    # Add the value of each bar at the top
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval + 0.05,
            round(yval, 2),  # text
            ha="center",
            va="bottom",
            rotation=0
        )
    # # Plot the chart with all settings.
    # plt.show()
    # Save the chart as a PNG file
    plt.savefig(
        "figures/4.2 A num of rows in tables.png",
        dpi=300,
        bbox_inches="tight"
    )
    plt.close()


def print_db_info(db_info):
    """Prints the table names and all column names of every table."""
    for table_name, column_names in db_info.items():
        print(f"Table name: {table_name}")
        print(f"Table columns:\n {column_names}\n")


def create_reports(db_info, conn, excluded_tables, num_rows=100000) -> None:
    """Creates reports using ydata_profiling.

    Creates the reports using ydata_profiling.
    In some cases, a table can have too many lines (several million
    lines).
    Then the table report creation can take a few hours. The num_rows
    parameter sets how many random lines you want to use to create
    they data_profiling report.

    See also a list of excluded_tables inside functions. Add the
    tables here that have too many lines. The num_rows parameter will
    be used only for "exlcuded" tables.
    """
    # excluded_tables = ["play_by_play"]
    empty_tables = []
    for table_name in db_info:
        # If the table is not too big.
        if table_name not in excluded_tables:
            df = pd.read_sql_query(
                f"SELECT * FROM {table_name}",
                conn
            )
            # If the table (dataframe) is not empty.
            if not df.empty:
                profile = ProfileReport(df, minimal=True)
                logger.info(
                    f"Generating the profile report for table {table_name}..."
                )
                # Save in the file because it is a more comfortable to
                # explore data opening the reports in a browser rather
                # than the notebook. Output in the notebook
                profile.to_widgets()
                # Save the output in the html-files.
                profile.to_file(f"reports/04/profile_report_{table_name}.html")
            # If the table (dataframe) is empty.
            else:
                empty_tables.append(table_name)
        # If the table is too big and inside a list of excluded_table.
        else:
            df = pd.read_sql_query(
                f"SELECT * "
                f"FROM {table_name} "
                f"ORDER BY RANDOM() "
                f"LIMIT {num_rows}",
                conn
            )
            # If the table (dataframe) is empty.
            if not df.empty:
                profile = ProfileReport(df, minimal=True)
                logger.info(
                    f"Generating the profile report for table {table_name}..."
                )
                # Output in the notebook
                profile.to_widgets()
                # Save the output in the html-files.
                profile.to_file(
                    f"reports/04/profile_report_{table_name}_{num_rows}_samples.html")
            # If the table (dataframe) is empty.
            else:
                empty_tables.append(table_name)
                # Log the main data.
    logger.info(
        f"The profile reports have been generated.\n"
        f"The following tables have been excluded (several million lines take "
        f"a few hours to create the report):\n{excluded_tables}\n"
        f"The empty dataframes for tables:\n{empty_tables}"
    )